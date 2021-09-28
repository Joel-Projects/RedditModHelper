import time
from datetime import datetime
from typing import Optional

import discord
from discord.ext.commands import Cog, Context
from discord_slash import ComponentContext
from discord_slash.cog_ext import cog_component, cog_slash
from discord_slash.model import ButtonStyle
from discord_slash.utils.manage_commands import create_option
from discord_slash.utils.manage_components import create_actionrow, create_button

from .utils import checks, db
from .utils import time as utime
from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.converters import NotFound, RedditorConverter
from .utils.utils import parse_sql

TIME_FORMAT = "%B %d, %Y at %I:%M:%S %p %Z"


class Users(db.Table, table_name="users"):
    id = db.PrimaryKeyColumn()
    user_id = db.Column(db.Integer(big=True), index=True, unique=True)
    username = db.Column(db.String, nullable=False)
    created_at = db.Column(db.Datetime(timezone=True), nullable=False)
    joined_at = db.Column(db.Datetime(timezone=True))
    status = db.Column(db.Status, default="'unverified'")
    link_message_id = db.Column(db.Integer(big=True))
    welcome_message_id = db.Column(db.Integer(big=True))


class ApprovalLog(db.Table, table_name="approval_log"):
    id = db.PrimaryKeyColumn()
    user_id = db.Column(
        db.ForeignKey("users", "user_id", sql_type=db.Integer(big=True)),
        index=True,
        nullable=False,
    )
    actor_id = db.Column(db.Integer(big=True), index=True, nullable=False)
    action_type = db.Column(db.ActionType, nullable=False)
    channel_id = db.Column(db.Integer(big=True))
    message_id = db.Column(db.Integer(big=True))
    actioned_at = db.Column(db.Datetime(timezone=True), default="NOW()")


class ApprovalMessages(db.Table, table_name="approval_messages"):
    id = db.PrimaryKeyColumn()
    user_id = db.Column(
        db.ForeignKey("users", "user_id", sql_type=db.Integer(big=True)),
        index=True,
        nullable=False,
    )
    message_id = db.Column(db.Integer(big=True), index=True, nullable=False, unique=True)


class PreRedditors(db.Table, table_name="pre_redditors"):
    id = db.PrimaryKeyColumn()
    redditor = db.Column(db.String, nullable=False, unique=True)
    actor_id = db.Column(db.Integer(big=True), index=True, nullable=False)
    status = db.Column(db.Status, nullable=False)
    timestamp = db.Column(db.Datetime(timezone=True), default="NOW()")


class Permissions(CommandCog, command_attrs={"hidden": True}):
    """A collection of Permission commands."""

    def __init__(self, bot):
        super().__init__(bot)
        self.approval_channel: discord.TextChannel = None
        self.approved_role: discord.Role = None
        self.dmz_channel: discord.TextChannel = None
        self.grandfather_role: discord.Role = None
        self.unverified_role: discord.Role = None
        self.verified_role: discord.Role = None
        self.unapproved_role: discord.Role = None
        self.auto_kick = True

    async def cog_before_invoke(self, context):
        await self.set_references(context.bot.snoo_guild)

    async def get_message_user(self, context, message_id):
        result = parse_sql(
            await self.sql.fetch("SELECT user_id FROM approval_messages WHERE message_id=$1", int(message_id)),
            fetch_one=True,
        )
        if result:
            return result.user_id
        else:
            await self.error_embed(context, "Something went wrong while fetching the user.")

    async def _check_authorized(self, context):
        is_authorized = bool(
            set(await self.get_bot_config("authorized_roles")).intersection({role.id for role in context.author.roles})
        )
        if not is_authorized:
            await self.error_embed(context, "You're not allowed to do that!")
        return is_authorized

    async def _button_pre_check(self, context: ComponentContext):
        await self.set_references(self.bot.snoo_guild)
        return await self._check_authorized(context)

    @cog_component()
    async def approve(self, context: ComponentContext):
        await context.defer()
        await self._action_user_button(context, "approve")

    async def _action_user_button(self, context, action):
        try:
            if await self._button_pre_check(context):
                user_id = await self.get_message_user(context, context.origin_message_id)
                await self.action_user(context, user_id, action, send_embed=False)
                user_info = await self.get_user(context.guild, user_id)
                if user_info and user_info.status in ["approved", "denied"]:
                    buttons = await self.generate_approval_buttons(user_id, user_info.status)
                    await context.origin_message.edit(embed=context.origin_message.embeds[0], components=buttons)
                else:
                    action_label = "approving" if action == "approve" else "denying"
                    await self.error_embed(context, f"Something went wong while {action_label} {user_info.username}.")
        except Exception as error:
            self.log.exception(error)

    async def generate_approval_buttons(self, user_id, status):
        button_mapping = {"approved": "approve", "whitelisted": "approve", "denied": "deny", "blacklisted": "deny"}
        button_config = {
            "approve": {"label": "Approve", "disabled": False},
            "deny": {"label": "Deny", "disabled": False},
        }
        if status in button_mapping:
            actor, timestamp = await self.get_actor_for_member(user_id)
            if isinstance(actor, discord.Member):
                actor = actor.name
            elif isinstance(actor, int) and actor == 0:
                actor = "grandfather"
            else:
                actor = "someone"
            button_config[button_mapping[status]]["label"] = f"{status.title()} by {actor} at {timestamp}"
            button_config[button_mapping[status]]["disabled"] = True
        buttons = [
            create_button(style=ButtonStyle.success, custom_id="approve", **button_config["approve"]),
            create_button(style=ButtonStyle.danger, custom_id="deny", **button_config["deny"]),
        ]
        return [create_actionrow(*buttons)]

    @cog_component()
    async def deny(self, context: ComponentContext):
        await context.defer()
        await self._action_user_button(context, "deny")

    @Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.set_references(self.bot.snoo_guild)
        await self.on_join(member)

    @staticmethod
    def get_incompatible(roles, *pairs):
        incompatible_pairs = []
        for role_a, role_b in pairs:
            if role_a in roles and role_b in roles:
                incompatible_pairs.append((role_a, role_b))
        return incompatible_pairs

    @staticmethod
    def has_roles(func, roles, *roles_to_check):
        return func([role in roles for role in roles_to_check])

    async def action_user(self, context, member, action, grandfathered=False, send_embed=True):
        if action in ["approve", "deny"]:
            if action == "approve":
                await self.approve_user(member, context.author, grandfathered=grandfathered, send_embed=send_embed)
            elif action == "deny":
                redditor = await self.get_redditor(None, member)
                await self.deny_user(member, redditor, context.author, context.message)
        member_str = member.mention if isinstance(member, discord.Member) else f"<@{member}>"
        await self.success_embed(
            context, f"Successfully {'approved' if action == 'approve' else 'denied'} {member_str}!"
        )

    async def action_users(self, context, user_ids, action):
        users = set(user_ids)
        failed = []
        for user in users:
            if isinstance(user, NotFound):
                failed.append(user)
            else:
                await self.action_user(context, user, action)
        if len(failed) == 1:
            await self.error_embed(context, f"Could not find user matching {failed[0].arg}")
        elif len(failed) > 1:
            failed_users = "\n".join([user.arg for user in failed])
            await self.error_embed(context, f"Could not find the following users:\n\n{failed_users}")

    async def approve_user(
        self, member, actor, approval_message=None, grandfathered=False, preemptive=False, send_embed=True
    ):
        try:
            roles_to_add = []
            if member:
                redditor = await self.get_redditor(None, member)
            if isinstance(member, discord.Member):
                if not redditor:
                    await self.error_embed(
                        self.approval_channel,
                        f"Unable to approve {member.mention} because they have not verified their reddit account yet.",
                    )
                    return
                redditor = await self.reddit.redditor(redditor, fetch=True)
                moderated_subreddits = await redditor.moderated()
                results = parse_sql(await self.sql.fetch("SELECT name, role_id FROM subreddits"))
                if results:
                    roles_to_add += [
                        self.bot.snoo_guild.get_role(result.role_id)
                        for result in results
                        if result.name in moderated_subreddits and self.bot.snoo_guild.get_role(result.role_id)
                    ]
                    await member.add_roles(self.approved_role, *roles_to_add)
                    await member.remove_roles(self.unverified_role, self.unapproved_role, self.grandfather_role)
            member = member.id if isinstance(member, discord.Member) else member
            preemptive_note = ""
            if preemptive:
                _, actor, timestamp = await self.check_pre_redditor(redditor)
                preemptive_note = f"whitelisted by {actor.mention} at {timestamp}"
            else:
                await self.sql.execute("DELETE FROM pre_redditors WHERE redditor=$1", redditor)
            note = (
                f"\nNote: This user was {'grandfathered in' if grandfathered else preemptive_note}."
                if preemptive or grandfathered
                else ""
            )
            if send_embed:
                await self.success_embed(self.approval_channel, f"Successfully approved {member.mention}!{note}")
            query_args = [member, 0 if grandfathered else actor.id, "approve"]
            await self.sql.execute("UPDATE users SET status=$1 WHERE user_id=$2", "approved", member)
            if approval_message:
                query = "INSERT INTO approval_log (user_id, actor_id, action_type, channel_id, message_id) VALUES ($1, $2, $3, $4, $5)"
                query_args += [approval_message.channel.id, approval_message.id]
            else:
                query = "INSERT INTO approval_log (user_id, actor_id, action_type) VALUES ($1, $2, $3)"
            await self.sql.execute(query, *query_args)
        except Exception as error:
            self.log.exception(error)

    async def check_existing_status(self, action, context, redditor):
        result = await self.sql.fetchval(
            "SELECT user_id FROM credential_store.user_verifications WHERE redditor=$1", redditor
        )
        if result:
            member_id = int(result)
            result = parse_sql(await self.sql.fetch("SELECT * FROM users WHERE user_id=$1", member_id), fetch_one=True)
            if result:
                if (result.status != "denied" and action == "denied") or (
                    result.status == "denied" and action == "approved"
                ):
                    in_server = context.guild.get_member(member_id)
                    note = (
                        f"\n\n**Note: Confirming this will __kick__ this user!**"
                        if action == "denied" and in_server
                        else ""
                    )
                    if result.status in ["approved", "denied"]:
                        actor, timestamp = await self.get_actor_for_member(member_id)
                        actor_str = (
                            actor.mention
                            if isinstance(actor, discord.Member)
                            else "grandfather"
                            if actor == 0
                            else f"<@{actor}>"
                        )
                        confirm = await context.prompt(
                            f"It appears that {actor_str} already {result.status} u/{redditor} on {timestamp}.\nDo you want to override?{note}"
                        )
                    elif result.status in ["verified", "unverified"]:
                        in_server = bool(context.guild.get_member(member_id))
                        confirm = await context.prompt(
                            f"It appears that u/{redditor} is {'already in the server and is' if in_server else ''} currently {result.status}.\nDo you want to override?{note if in_server else ''}"
                        )
                    if confirm:
                        if action == "denied":
                            member = context.guild.get_member(member_id)
                            if member:
                                await self.deny_user(member, redditor, context.author)
                                await self.success_embed(context, f"Successfully kicked {member.mention}!")
                        await self.sql.execute("UPDATE users SET status=$1 WHERE user_id=$2", action, member_id)
                        return True
                    else:
                        return False
        return True

    async def check_pre_redditor(self, redditor):
        result = parse_sql(
            await self.sql.fetch("SELECT * FROM pre_redditors WHERE redditor=$1", str(redditor)), fetch_one=True
        )
        if result:
            status = result.status
            actor = self.bot.snoo_guild.get_member(result.actor_id) or result.actor_id
            timestamp = result.timestamp.astimezone().strftime(TIME_FORMAT)
            return status, actor, timestamp
        else:
            return [None] * 3

    async def deny_user(self, member, redditor, actor=None, approval_message=None, preemptive=False, previous=False):
        try:
            if previous and isinstance(member, discord.Member):
                actor, _ = await self.get_actor_for_member(member.id)
                if not self.bot.debug and self.auto_kick:
                    if isinstance(actor, discord.Member):
                        await member.kick(
                            reason=f"Previously denied by {actor.name}#{actor.discriminator} ({actor.id})"
                        )
                    else:
                        await member.kick(reason=f"Previously denied by <@{actor}>")
            else:
                member = member.id if isinstance(member, discord.Member) else member
                if preemptive:
                    _, actor, _ = await self.check_pre_redditor(redditor)
                    deny_type = "Blacklisted"
                else:  # only clear if denied and don't clear blacklist status
                    await self.sql.execute("DELETE FROM pre_redditors WHERE redditor=$1", redditor)
                    deny_type = "Denied"
                if not self.bot.debug:
                    await member.kick(reason=f"{deny_type} by {actor.name}#{actor.discriminator} ({actor.id})")
                await self.sql.execute("UPDATE users SET status=$1 WHERE user_id=$2", "denied", member)
                query_args = [member, actor.id, "deny"]
                if approval_message:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type, channel_id, message_id) VALUES ($1, $2, $3, $4, $5)"
                    query_args += [approval_message.channel.id, approval_message.id]
                else:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type) VALUES ($1, $2, $3)"
                await self.sql.execute(query, *query_args)
        except Exception:
            await self.error_embed(self.approval_channel, f"Failed to kick <@{member}>.")

    async def execute_preemptive(self, member, redditor):
        preemptive_status, actor, timestamp = await self.check_pre_redditor(redditor)
        if preemptive_status == "denied":
            preemptive_status = "blacklisted"
            await self.deny_user(member, redditor, actor, preemptive=True)
        elif preemptive_status == "approved":
            preemptive_status = "whitelisted"
            await self.approve_user(member, actor, preemptive=True, send_embed=False)
        return preemptive_status, actor, timestamp

    async def get_actor_for_member(self, member_id):
        logs = parse_sql(
            await self.sql.fetch(
                "SELECT * FROM approval_log WHERE user_id=$1 ORDER BY actioned_at DESC LIMIT 1",
                member_id,
            )
        )
        if logs:
            actor = self.bot.snoo_guild.get_member(
                logs[0].actor_id,
            )
            if not actor:
                actor = logs[0].actor_id
        else:
            return None, None
        return actor, logs[0].actioned_at.astimezone().strftime(TIME_FORMAT)

    async def get_user(self, guild, member_id, return_member=False):
        if member_id > 2147483647:
            query = "SELECT * FROM users WHERE user_id=$1"
        else:
            query = "SELECT * FROM users WHERE id=$1"
        result = parse_sql(await self.sql.fetch(query, member_id), fetch_one=True)
        if result:
            return guild.get_member(result.user_id) if return_member else result
        else:
            return None

    async def insert_user(self, member):
        self.sql = self.bot.pool
        joined_at = None
        if hasattr(member, "joined_at"):
            joined_at = member.joined_at
        result = parse_sql(await self.sql.fetch("SELECT * FROM users WHERE user_id=$1", member.id), fetch_one=True)
        if result:
            if result.joined_at is None and joined_at:
                await self.sql.execute(
                    "UPDATE users SET joined_at=$1, username=$2 WHERE user_id=$3", joined_at, member.name, member.id
                )
        else:
            await self.sql.execute(
                f"INSERT INTO users (user_id, username, created_at, joined_at) VALUES ($1, $2, $3, $4)",
                member.id,
                member.name,
                member.created_at,
                joined_at,
            )

    async def on_join(self, member: discord.Member):
        if member.bot:
            return
        await member.add_roles(self.unverified_role)
        self.sql = self.bot.pool
        await self.insert_user(member)
        redditor = await self.get_redditor(None, member)
        if redditor:
            await member.add_roles(self.verified_role, self.unapproved_role)
            await member.remove_roles(self.unverified_role)
            message = await self.success_embed(
                self.dmz_channel,
                f"Verified u/{redditor} successfully!",
            )
            welcome_message = await self.dmz_channel.send(
                f"Welcome {member.mention}! You have already verified your account.\nNote: You may have to wait for approval before your able to access the rest of the server."
            )
            preemptive_status = await self.execute_preemptive(member, redditor)
            if all(preemptive_status):
                await self.send_approval_request(member, redditor, preemptive_status)
            else:
                result = await self.get_user(member.guild, member.id)
                if result.status == "approved":
                    await member.add_roles(self.approved_role)
                    await member.remove_roles(self.unapproved_role, self.grandfather_role)
                elif result.status == "denied":
                    await self.deny_user(member, redditor, previous=True)
                elif result.status == "unverified":
                    await self.sql.execute("UPDATE users set status=$1 WHERE user_id=$2", "verified", member.id)
                await self.send_approval_request(member, redditor)
        else:
            embed = discord.Embed(
                title="Reddit Account Verification",
                description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
            )
            verification = self.bot.credmgr.userVerification.create(str(member.id), self.bot.credmgr_bot.redditApp)
            embed.add_field(
                name="Authenticate Here:",
                value=self.bot.credmgr_bot.redditApp.genAuthUrl(userVerification=verification),
                inline=True,
            )
            message = await self.dmz_channel.send(embed=embed)
            welcome_message = await self.dmz_channel.send(
                f"Welcome {member.mention}! Send `.done` after you have verified your reddit account using the above link."
            )
        if message and welcome_message:
            await self.sql.execute(
                "UPDATE users SET link_message_id=$1, welcome_message_id=$2 WHERE user_id=$3",
                message.id,
                welcome_message.id,
                member.id,
            )

    async def pre_action_user(self, context, redditor, action):
        result = parse_sql(
            await self.sql.fetch("SELECT * FROM pre_redditors WHERE redditor=$1", redditor), fetch_one=True
        )
        if result:
            actor = context.guild.get_member(
                result.actor_id,
            )
            timestamp = result.timestamp.astimezone().strftime(TIME_FORMAT)
            confirm = await context.prompt(
                f"It appears that {actor.mention} already {'whitelisted' if result.status == 'approved' else 'blacklisted'} u/{result.redditor} on {timestamp}.\nDo you want to override?"
            )
            if not confirm:
                return
            result = await self.check_existing_status(action, context, redditor)
            if result:
                await self.sql.execute(
                    "UPDATE pre_redditors SET status=$1, timestamp=NOW() WHERE redditor=$2", action, redditor
                )
        else:
            result = await self.check_existing_status(action, context, redditor)
            if result:
                await self.sql.execute(
                    "INSERT INTO pre_redditors (redditor, actor_id, status) VALUES ($1, $2, $3)",
                    redditor,
                    context.author.id,
                    action,
                )
        if result:
            await self.success_embed(
                context, f"Successfully {'whitelisted' if action == 'approved' else 'blacklisted'} u/{redditor}!"
            )

    async def send_approval_request(self, member, redditor, preemptive_status=None):
        result = await self.get_user(member.guild, member.id)
        if result:
            previous_action = None
            actor = None
            timestamp = None
            if result.status in ["approved", "denied"]:
                actor, timestamp = await self.get_actor_for_member(member.id)
                if isinstance(actor, discord.Member):
                    actor = f"{actor.name}#{actor.discriminator} ({actor.mention})"
                elif isinstance(actor, int):
                    actor = "grandfather" if actor == 0 else f"<@{actor}>"
                else:
                    actor = "someone"
                previous_action = result.status
            if preemptive_status:
                previous_action, actor, timestamp = preemptive_status
            redditor = await self.reddit.redditor(redditor, fetch=True)
            if previous_action:
                note = (
                    f"\n***Note: If you deny this user they will be immediately kicked.***"
                    if previous_action in ["approved", "whitelisted"]
                    else "\nThey will be able to join again after you approve them. Approving them will clear their blacklisted status."
                )
                embed = discord.Embed(
                    title=f"{'Already ' if previous_action in ['approved', 'denied'] else ''} {previous_action.title()} User Alert",
                    description=f"u/{redditor} was already {previous_action} by {actor} on {timestamp}{' and has been kicked' if previous_action in ['denied' 'blacklisted'] and self.auto_kick else ''}.\n{note}",
                    color=discord.Color.orange(),
                )
            else:
                embed = discord.Embed(
                    title="New User",
                    description=f"{member.mention}\n***Note: If you deny this user they will be immediately kicked.***",
                    color=discord.Color.green(),
                )
            embed.set_author(
                name=redditor.name,
                url=f"https://www.reddit.com/user/{redditor.name}",
                icon_url=redditor.icon_img,
            )
            embed.set_thumbnail(url=str(member.avatar_url))
            embed.add_field(
                name="Joined Reddit",
                value=time.strftime(TIME_FORMAT, time.localtime(redditor.created_utc))
                + f"\n({utime.human_timedelta(datetime.utcfromtimestamp(redditor.created_utc).astimezone(), accuracy=1)})",
            )
            embed.add_field(
                name="Joined Discord",
                value=member.created_at.astimezone().strftime(TIME_FORMAT)
                + f"\n({utime.human_timedelta(member.created_at.astimezone(), accuracy=1)})",
            )
            embed.add_field(
                name="Joined Server",
                value=member.joined_at.astimezone().strftime(TIME_FORMAT)
                + f"\n({utime.human_timedelta(member.joined_at.astimezone(), accuracy=1)})",
            )
            embed.set_footer(text=time.strftime(TIME_FORMAT, time.localtime()))
            _, _, sub_count, subreddits, subscribers, _ = await self.get_and_calculate_subs(str(redditor))
            embed.add_field(name="Subreddit Count", value=f"{sub_count:,}")
            embed.add_field(name="Subscriber Count", value=f"{subscribers:,}")
            value_string = (
                "\n".join(
                    [
                        f"{sub_rank}. {subreddit[0]}: {subreddit[1]:,}"
                        for sub_rank, subreddit in enumerate(subreddits[:20], 1)
                    ]
                )
                if subreddits
                else "This user does not moderate any subreddits."
            )
            embed.add_field(name="Top 20 Subreddits", value=value_string, inline=False)
            buttons = await self.generate_approval_buttons(member.id, previous_action)
            message = await self.approval_channel.send(embed=embed, components=buttons)
            await self.sql.execute(
                "INSERT INTO approval_messages (user_id, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                member.id,
                message.id,
            )
            return message
        else:
            self.log.error(f"Something went wrong getting approval for {redditor}")

    async def send_pre_redditor_embed(self, redditor, actor, action, timestamp):
        channel = self.approval_channel
        if isinstance(actor, discord.Member):
            actor = actor.mention
        elif isinstance(actor, int):
            actor = f"<@{actor}>"
        if action == "denied":
            description = "just tried joining but was kicked automatically due to being blacklisted"
            status = "blacklist"
        else:
            description = "was automatically approved due to being whitelisted"
            status = "whitelist"
        embed = discord.Embed(
            title=f"{status.title()}ed User Alert",
            description=f"u/{redditor} {description} by {actor} on {timestamp}.\n"
            f"To remove them from the {status} do `.rem{status} {redditor}`.",
            color=discord.Color.orange(),
        )
        embed.set_footer(text=time.strftime(TIME_FORMAT, time.localtime()))
        return await channel.send(embed=embed)

    async def set_references(self, guild: discord.Guild):
        objects = {
            "approval_channel": await self.get_bot_config("approval_channel_debug")
            if self.bot.debug
            else await self.get_bot_config("approval_channel"),
            "approved_role": await self.get_bot_config("approved_role"),
            "dmz_channel": await self.get_bot_config("approval_channel_debug")
            if self.bot.debug
            else await self.get_bot_config("dmz_channel"),
            "grandfather_role": await self.get_bot_config("grandfather_role"),
            "unverified_role": await self.get_bot_config("unverified_role"),
            "verified_role": await self.get_bot_config("verified_role"),
            "unapproved_role": await self.get_bot_config("unapproved_role"),
        }
        for key, value in objects.items():
            setattr(self, key, discord.utils.get(guild.roles + guild.channels, id=value))
        settings = {
            "auto_kick": await self.get_bot_config("auto_kick"),
        }
        for key, value in settings.items():
            setattr(self, key, value)

    @command()
    @checks.authorized_roles()
    async def _on_join(self, context):
        member = context.guild.get_member(857055123078119474)
        await member.remove_roles(
            self.approved_role, self.grandfather_role, self.verified_role, self.unapproved_role, self.unverified_role
        )
        await self.on_join(member)

    @command()
    @checks.authorized_roles()
    async def _on_verify(self, context):
        member = context.guild.get_member(857055123078119474)
        await member.remove_roles(self.approved_role, self.grandfather_role, self.unverified_role)
        await member.add_roles(self.unapproved_role, self.verified_role)
        await self.sql.execute("UPDATE users SET status='verified' WHERE user_id=$1", 857055123078119474)
        await self.send_approval_request(member, "Lil_SpazTest")

    @command()
    @checks.is_admin()
    async def adjuser(self, context: Context):
        for user in context.guild.members:
            redditor = await self.get_redditor(context, user)
            roles = set(user.roles)
            if redditor:
                if self.has_roles(all, roles, self.approved_role, self.verified_role) and not self.has_roles(
                    any,
                    roles,
                    self.unapproved_role,
                    self.unverified_role,
                    self.grandfather_role,
                ):
                    print(f"{user.name} is good")
                    continue
                if self.verified_role not in roles:
                    await user.add_roles(self.verified_role)
                    roles.add(self.verified_role)
                    print(f"+verified {user.name}")
                    if self.unverified_role in roles:
                        await user.remove_roles(self.unverified_role)
                        roles.remove([self.unverified_role])

                if self.grandfather_role in roles:
                    if self.approved_role not in roles:
                        await user.add_roles(self.approved_role)
                        roles.add(self.approved_role)
                        print(f"+approved {user.name}")
                    if self.unverified_role in roles:
                        await user.remove_roles(self.unverified_role)
                        roles.remove(self.unverified_role)
                        print(f"-unverified {user.name}")
                    if self.unapproved_role in roles:
                        await user.remove_roles(self.unapproved_role)
                        roles.remove(self.unapproved_role)
                        print(f"-unapproved {user.name}")
                    if self.grandfather_role in roles:
                        await user.remove_roles(self.grandfather_role)
                        roles.remove(self.grandfather_role)
                        print(f"-grandfather {user.name}")
            else:
                if self.verified_role in roles:
                    await user.remove_roles(self.verified_role)
                    roles.remove(self.verified_role)
                    print(f"-verified {user.name}")
                    if self.unverified_role not in roles:
                        await user.add_roles(self.unverified_role)
                        roles.add(self.unverified_role)
                        print(f"+unverified {user.name}")
            incompatible_roles = self.get_incompatible(
                roles,
                (self.approved_role, self.grandfather_role),
                (self.approved_role, self.unapproved_role),
                (self.verified_role, self.unverified_role),
            )
            for role_a, role_b in incompatible_roles:
                await user.remove_roles(role_b)
                roles.remove(role_b)
                print(f"-{role_b.name} {user.name}")

    # @command()
    # @checks.authorized_roles()
    # async def approve(self, context, *user_ids: UserIDConverter):
    #     """Approve a user to access this server.
    #
    #     Params:
    #
    #     user_ids: One or more users to approve. This can be the number in the approval embed or a discord user ID or a user mention.
    #
    #     Examples:
    #
    #     `.approve 1` approves user 1. This is the number provided in the approval embed.
    #     `.approve 1 2` same as previous but also approves user 2.
    #     `.approve 393801572858986496` approves user with discord user ID 393801572858986496.
    #     `.approve @N8theGr8` this approves N8theGr8.
    #     """
    #     await self.action_users(context, user_ids, "approve")

    @command()
    @checks.authorized_roles()
    async def assignunv(self, context):
        for user in context.guild.members:
            await self.insert_user(user)
            redditor = await self.get_redditor(context, user)
            if self.approved_role not in user.roles:
                if redditor:
                    await user.add_roles(self.verified_role, self.approved_role)
                    await user.remove_roles(self.grandfather_role, self.unverified_role)
                    self.log.info(f"Added approved_role removed grandfather_role role to {user}")
                else:
                    await user.add_roles(self.unverified_role, self.grandfather_role)
                    await user.remove_roles(self.approved_role, self.verified_role)
                    self.log.info(f"Added grandfather_role role to {user}")
            else:
                if redditor:
                    await user.add_roles(self.verified_role, self.approved_role)
                    await user.remove_roles(self.unverified_role, self.grandfather_role)
                else:
                    await user.add_roles(self.unverified_role, self.grandfather_role)
                self.log.info(f"{user} already approved")

    # todo: convert this to slash command
    @command()
    @checks.authorized_roles()
    async def blacklist(self, context, *usernames: RedditorConverter):
        """Preemptively deny a user from accessing this server.

        Params:

        usernames: One or more redditors to blacklist. This is the redditor's username, case-insensitive.

        Examples:

        `.blacklist spez` blacklist user spez.
        `.blacklist spez N8theGr8` same as previous but also blacklists N8theGr8.
        """
        for user in usernames:
            await self.pre_action_user(context, user, "denied")

    # @command()
    # @checks.authorized_roles()
    # async def deny(self, context, *user_ids: UserIDConverter):
    #     """Deny a user to access this server.
    #
    #     Params:
    #
    #     user_ids: One or more users to deny. This can be the number in the approval embed or a discord user id or a user mention.
    #
    #     Examples:
    #
    #     `.deny 1` denies user 1. This is the number provided in the approval embed.
    #     `.deny 1 2` same as previous but also denies user 2.
    #     `.deny 393801572858986496` denies user with discord user id 393801572858986496.
    #     `.deny @N8theGr8` this denies N8theGr8.
    #     """
    #     await self.action_users(context, user_ids, "deny")

    @command()
    async def done(self, context: Context, userid: Optional[int]):
        if context.guild:
            if userid:
                member = discord.utils.get(context.guild.members, id=userid)
            else:
                member = context.author
        else:
            member = discord.utils.get(context.bot.snoo_guild.members, id=context.author.id)
        if member:
            redditor = await self.get_redditor(context, member)
            if not redditor:
                await self.error_embed(
                    context,
                    "I was unable to verify your reddit account, please try authorizing with the link above again.",
                )
                return
            preemptive_status = await self.execute_preemptive(member, redditor)
            if all(preemptive_status):
                await self.send_approval_request(member, redditor, preemptive_status)
            else:
                result = await self.get_user(member.guild, member.id)
                if result:
                    if result.status == "approved":
                        await member.add_roles(self.approved_role, self.verified_role)
                        await member.remove_roles(self.unverified_role, self.unapproved_role)
                        await self.success_embed(context, f"Verified u/{redditor} successfully!")
                    elif result.status == "denied":
                        await self.deny_user(member, redditor, previous=True)
                        await self.send_approval_request(member, redditor)
                    else:
                        await member.add_roles(self.unapproved_role, self.verified_role)
                        await member.remove_roles(self.unverified_role)
                        result = parse_sql(
                            await self.sql.fetch(
                                "UPDATE users set status=$1 WHERE user_id=$2 RETURNING id, link_message_id, welcome_message_id",
                                "verified",
                                member.id,
                            ),
                            fetch_one=True,
                        )
                        if result:
                            try:
                                messages_to_delete = [
                                    self.dmz_channel.get_partial_message(getattr(result, attr))
                                    for attr in ["link_message_id", "welcome_message_id"]
                                    if getattr(result, attr)
                                ]
                                await context.message.delete()
                                for message in messages_to_delete:
                                    await message.delete()
                            except Exception:
                                pass
                            if self.grandfather_role in member.roles:
                                await self.action_user(context, member, "approve", True)
                                await self.success_embed(
                                    context,
                                    f"Verified u/{redditor} successfully!",
                                )
                                await self.send_approval_request(member, redditor)
                                return
                            note = (
                                "\nNote: you will have to wait for approval before you are allowed to access the server."
                                if self.approved_role not in member.roles
                                else ""
                            )
                            await self.success_embed(
                                context,
                                f"Verified u/{redditor} successfully!{note}",
                            )
                            await self.send_approval_request(member, redditor)
                        else:
                            await self.error_embed(
                                context,
                                "I was unable to verify your reddit account, please send `.verify` to retry verification.",
                            )
                else:
                    await self.error_embed(
                        context,
                        "I was unable to verify your reddit account, please send `.verify` to retry verification.",
                    )
        else:
            await self.error_embed(
                context,
                "You must be a member of the server to use this command.",
            )

    @command()
    @checks.authorized_roles()
    async def remblacklist(self, context, *usernames: RedditorConverter):
        """Removes users from blacklist.

        Params:

        usernames: One or more redditors to remove from the blacklist. This is the redditor's username, case insensitive.

        Examples:

        `.remblacklist spez` removes user spez from the blacklist.
        `.remblacklist spez N8theGr8` same as previous but also removes N8theGr8 from the blacklist.
        """
        success = []
        not_blacklisted = []
        failed = []
        for user in usernames:
            try:
                result = await self.sql.execute("DELETE FROM pre_redditors WHERE status='denied' AND redditor=$1", user)
                (success if result == "DELETE 1" else not_blacklisted).append(user)
            except Exception:
                failed.append(user)
        if len(success) == 1:
            await self.success_embed(context, f"Successfully removed u/{success[0]} from the blacklist.")
        elif len(success) > 1:
            removed = "\n".join([f"u/{user}" for user in success])
            await self.success_embed(
                context, f"Successfully removed the following users from the blacklist:\n\n{removed}"
            )
        if len(failed) == 1:
            await self.error_embed(context, f"Failed to removed u/{failed[0]} from the blacklist.")
        elif len(failed) > 1:
            removed = "\n".join([f"u/{user}" for user in failed])
            await self.error_embed(context, f"Failed removed the following users from the blacklist:\n\n{removed}")
        if len(not_blacklisted) == 1:
            await self.error_embed(context, f"u/{not_blacklisted[0]} is not blacklisted.")
        elif len(not_blacklisted) > 1:
            removed = "\n".join([f"u/{user}" for user in not_blacklisted])
            await self.error_embed(context, f"The following users are not blacklisted:\n\n{removed}")

    @command()
    @checks.authorized_roles()
    async def remwhitelist(self, context, *usernames: RedditorConverter):
        """Removes users from whitelist.

        Params:

        usernames: One or more redditors to remove from the whitelist. This is the redditor's username, case insensitive.

        Examples:

        `.remwhitelist spez` removes user spez from the whitelist.
        `.remwhitelist spez N8theGr8` same as previous but also removes N8theGr8 from the whitelist.
        """
        success = []
        not_whitelisted = []
        failed = []
        for user in usernames:
            try:
                result = await self.sql.execute(
                    "DELETE FROM pre_redditors WHERE status='approved' AND redditor=$1", user
                )
                (success if result == "DELETE 1" else not_whitelisted).append(user)
            except Exception:
                failed.append(user)
        if len(success) == 1:
            await self.success_embed(context, f"Successfully removed u/{success[0]} from the whitelist.")
        elif len(success) > 1:
            removed = "\n".join([f"u/{user}" for user in success])
            await self.success_embed(
                context, f"Successfully removed the following users from the whitelist:\n\n{removed}"
            )
        if len(failed) == 1:
            await self.error_embed(context, f"Failed to removed u/{failed[0]} from the whitelist.")
        elif len(failed) > 1:
            removed = "\n".join([f"u/{user}" for user in failed])
            await self.error_embed(context, f"Failed removed the following users from the whitelist:\n\n{removed}")
        if len(not_whitelisted) == 1:
            await self.error_embed(context, f"u/{not_whitelisted[0]} is not whitelisted.")
        elif len(not_whitelisted) > 1:
            removed = "\n".join([f"u/{user}" for user in not_whitelisted])
            await self.error_embed(context, f"The following users are not whitelisted:\n\n{removed}")

    @command(name="verify")
    async def _verify(self, context, *args):
        await context.send("This command has been converted to a slash command: `/verify`")

    @cog_slash(
        options=[
            create_option(
                "member", "Discord user to verify. You must be authorized to specify this.", discord.Member, False
            ),
        ]
    )
    async def verify(self, context, member=None):
        """Manually verify your account Reddit account."""
        await context.defer(hidden=True)
        users = await self.check_multiple_auth(context, "member", [member or context.author], context.author)
        if not users:
            member = context.author
        else:
            member = users[0]
        await self.insert_user(member)
        embed = discord.Embed(
            title="Reddit Account Verification",
            description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
        )
        verification = self.bot.credmgr.userVerification.create(str(member.id), self.bot.credmgr_bot.redditApp)
        embed.add_field(
            name="Authenticate Here:",
            value=self.bot.credmgr_bot.redditApp.genAuthUrl(userVerification=verification),
            inline=True,
        )
        try:
            await context.send("Please check your DMs.", hidden=True)
            await member.send(embed=embed)
            await member.send(f"Send `.done` after you have verified your reddit account using the above link.")
        except discord.Forbidden:
            await context.send(
                "I was not able to send you a direct message for verification, please allow direct messages from server members and try again.",
                hidden=True,
            )

    # todo: convert this to slash command
    @command()
    @checks.authorized_roles()
    async def whitelist(self, context, *usernames: RedditorConverter):
        """Preemptively approve a user.

        Params:

        usernames: One or more redditors to pre-approve. This is the redditor's username, case-insensitive.

        Examples:

        `.whitelist spez` pre-approves user spez.
        `.whitelist spez N8theGr8` same as previous but also approves N8theGr8.
        """
        for user in usernames:
            await self.pre_action_user(context, user, "approved")


def setup(bot):
    bot.add_cog(Permissions(bot))
