import time
from datetime import datetime
from typing import Optional

import discord
from discord.ext.commands import Cog, Context
from discord_slash import ComponentContext
from discord_slash.cog_ext import cog_component, cog_context_menu, cog_slash, cog_subcommand, permission
from discord_slash.context import InteractionContext
from discord_slash.model import ButtonStyle
from discord_slash.utils.manage_commands import create_option, generate_permissions, remove_all_commands
from discord_slash.utils.manage_components import create_actionrow, create_button

from .utils import checks, db
from .utils import time as utime
from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.utils import EmbedType, generate_result_embed, parse_sql

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
    join_count = db.Column(db.Integer(), default=0)
    first_joined_at = db.Column(db.Datetime(timezone=True))


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


class VerificationMessages(db.Table, table_name="verification_messages"):
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

    # noinspection PyTypeChecker
    def __init__(self, bot):
        super().__init__(bot)
        self.admin_category: discord.CategoryChannel = None
        self.approval_channel: discord.TextChannel = None
        self.approved_role: discord.Role = None
        self.denied_role: discord.Role = None
        self.dmz_channel: discord.TextChannel = None
        self.grandfather_role: discord.Role = None
        self.unverified_role: discord.Role = None
        self.verified_role: discord.Role = None
        self.unapproved_role: discord.Role = None

    @Cog.listener()
    async def on_member_join(self, member):
        await self.on_join(member)

    @Cog.listener()
    async def on_ready(self):
        references = {
            "approval_channel": await self.get_bot_config(f"approval_channel{'_debug' if self.bot.debug else ''}"),
            "admin_category": await self.get_bot_config("admin_category"),
            "approved_role": await self.get_bot_config("approved_role"),
            "denied_role": await self.get_bot_config("denied_role"),
            "dmz_channel": await self.get_bot_config("approval_channel_debug" if self.bot.debug else "dmz_channel"),
            "grandfather_role": await self.get_bot_config("grandfather_role"),
            "unapproved_role": await self.get_bot_config("unapproved_role"),
            "unverified_role": await self.get_bot_config("unverified_role"),
            "verified_role": await self.get_bot_config("verified_role"),
        }
        for key, value in references.items():
            setattr(
                self,
                key,
                discord.utils.get(
                    self.bot.snoo_guild.roles + self.bot.snoo_guild.channels + self.bot.snoo_guild.categories, id=value
                ),
            )

    @staticmethod
    async def update_roles(member: discord.Member, add_roles=None, remove_roles=None):
        if add_roles:
            add_roles = add_roles if isinstance(add_roles, list) else [add_roles]
            await member.add_roles(*add_roles)
        if remove_roles:
            remove_roles = remove_roles if isinstance(remove_roles, list) else [remove_roles]
            await member.remove_roles(*remove_roles)

    async def get_message_user(self, context, message_id):
        result = parse_sql(
            await self.sql.fetch("SELECT user_id FROM approval_messages WHERE message_id=$1", int(message_id)),
            fetch_one=True,
        )
        if result:
            return await self.bot.get_or_fetch_member(self.bot.snoo_guild, result.user_id)
        else:
            await self.error_embed(context, "Something went wrong while fetching the user.")

    async def _check_authorized(self, context):
        is_authorized = bool(
            set(await self.get_bot_config("authorized_roles")).intersection({role.id for role in context.author.roles})
        )
        if not is_authorized:
            await self.error_embed(context, "You're not allowed to do that!")
        return is_authorized

    @cog_component()
    async def approve(self, context: ComponentContext):
        await context.defer()
        await self._action_user_button(context, "approve")

    @cog_component()
    async def deny(self, context: ComponentContext):
        await context.defer()
        await self._action_user_button(context, "deny")

    @cog_component()
    async def done(self, context: ComponentContext):
        await context.defer(hidden=True)
        member = context.author
        redditor = await self.get_redditor(member)
        if not redditor:
            await context.send(
                embed=generate_result_embed(
                    "I was unable to verify your reddit account, please try authorizing with the link above again.",
                    EmbedType.error,
                    contact_me=True,
                )
            )
            return
        if not await self.execute_preemptive(member, redditor):
            result = await self.get_user_info(member.id)
            if result:
                if result.status == "approved":
                    await self.approve_user(member, redditor, None, previous=True)
                elif result.status == "denied":
                    await self.deny_user(context, member, redditor, previous=True)
                else:
                    await self.set_verified(member)
                    if self.grandfather_role in member.roles:
                        await self.approve_user(member, redditor, None, grandfathered=True)
                note = (
                    "\nNote: you will have to wait for approval before you are allowed to access the server."
                    if self.approved_role not in member.roles
                    else ""
                )
                await context.send(
                    embed=generate_result_embed(f"Verified u/{redditor} successfully!{note}"), hidden=True
                )
                await self.send_approval_request(member, redditor)
            else:
                await context.send(
                    embed=generate_result_embed(
                        "I was unable to verify your reddit account, please send `/verify` to retry verification.",
                        EmbedType.error,
                        contact_me=True,
                    )
                )

    async def _action_user_button(self, context, action):
        try:
            if await self._check_authorized(context):
                user = await self.get_message_user(context, context.origin_message_id)
                await self.action_user(context, user, action, send_embed=False)
                user_info = await self.get_user_info(user.id)
                if user_info and user_info.status in ["approved", "denied"]:
                    buttons = await self.generate_approval_buttons(user.id, user_info.status)
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

    async def action_user(self, context, member, action, grandfathered=False, send_embed=True, hidden=False):
        if action in ["approve", "deny"]:
            redditor = await self.get_redditor(member)
            if action == "approve":
                await self.approve_user(
                    member, redditor, context.author, grandfathered=grandfathered, send_embed=send_embed
                )
            elif action == "deny":
                await self.deny_user(context, member, redditor, context.author, context.message, hidden=hidden)
        if not grandfathered:
            member_str = member.mention if isinstance(member, discord.Member) else f"<@{member}>"
            await context.send(
                embed=generate_result_embed(
                    f"Successfully {'approved' if action == 'approve' else 'denied'} {member_str}!"
                ),
                hidden=hidden,
            )

    # todo: maybe use context and hidden
    async def approve_user(
        self,
        member,
        redditor,
        actor,
        approval_message=None,
        grandfathered=False,
        preemptive=False,
        previous=False,
        send_embed=True,
    ):
        try:
            roles_to_add = [self.approved_role]
            roles_to_remove = [self.unapproved_role, self.denied_role]
            if isinstance(member, discord.Member):
                redditor = await self.reddit.redditor(redditor, fetch=True)
                moderated_subreddits = await redditor.moderated()
                results = parse_sql(await self.sql.fetch("SELECT name, role_id FROM subreddits"))
                if results:
                    roles_to_add += [
                        self.bot.snoo_guild.get_role(result.role_id)
                        for result in results
                        if result.name in moderated_subreddits and self.bot.snoo_guild.get_role(result.role_id)
                    ]
                    roles_to_remove += [
                        self.bot.snoo_guild.get_role(result.role_id)
                        for result in results
                        if result.name not in moderated_subreddits and self.bot.snoo_guild.get_role(result.role_id)
                    ]
                await self.update_roles(member, add_roles=roles_to_add, remove_roles=roles_to_remove)
            member = member.id if isinstance(member, discord.Member) else member
            note = "\nNote: This user was " if all([redditor, preemptive, grandfathered, previous]) else ""
            if redditor:
                if preemptive:
                    _, actor, timestamp = await self.check_pre_redditor(redditor)
                    note += f"whitelisted by {actor.mention} at {timestamp}."
                elif grandfathered:
                    note += "grandfathered in."
                elif previous:
                    actor, timestamp = await self.get_actor_for_member(member)
                    if isinstance(actor, int):
                        if actor == 0:
                            note += f"grandfathered in at {timestamp}."
                        else:
                            note += f"previously approved by {actor} at {timestamp}."
                    elif isinstance(actor, discord.Member):
                        note += f"previously approved by {actor.mention} at {timestamp}."
                    else:
                        note += f"previously approved by someone."
                else:
                    await self.sql.execute("DELETE FROM pre_redditors WHERE redditor=$1", str(redditor))
            if send_embed:
                await self.success_embed(
                    self.approval_channel, f"Successfully {'re' if previous else ''}approved <@{member}>!{note}"
                )
            if not previous or preemptive or grandfathered:
                query_args = [member, 0 if grandfathered else actor.id, "approve"]
                await self.sql.execute(
                    "UPDATE redditmodhelper.users SET status=$1 WHERE user_id=$2", "approved", member
                )
                if approval_message:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type, channel_id, message_id) VALUES ($1, $2, $3, $4, $5)"
                    query_args += [approval_message.channel.id, approval_message.id]
                else:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type) VALUES ($1, $2, $3)"
                await self.sql.execute(query, *query_args)
        except Exception as error:
            message = f"Failed to approve <@{member.id if isinstance(member, discord.Member) else member}>."
            await self.error_embed(self.approval_channel, message)

    async def deny_user(
        self,
        context,
        member,
        redditor,
        actor=None,
        approval_message=None,
        preemptive=False,
        previous=False,
        hidden=False,
        send_embed=True,
    ):
        try:
            if previous and isinstance(member, discord.Member):
                actor, _ = await self.get_actor_for_member(member.id)
                if await self.get_bot_config("auto_kick"):
                    if not self.bot.debug:
                        if isinstance(actor, discord.Member):
                            await member.kick(
                                reason=f"Previously denied by {actor.name}#{actor.discriminator} ({actor.id})"
                            )
                        else:
                            await member.kick(reason=f"Previously denied by <@{actor}>")
                    await self.success_embed(
                        self.approval_channel, f"Successfully kicked {member.mention} from the server!"
                    )
                else:
                    await self.update_roles(member, add_roles=self.denied_role, remove_roles=self.unapproved_role)
                    await self.success_embed(self.approval_channel, f"Successfully redenied {member.mention}!")
            else:
                if preemptive:
                    _, actor, timestamp = await self.check_pre_redditor(redditor)
                    deny_type = "Blacklisted"
                    note = f" They were {deny_type.lower()} by {actor} at {timestamp}."
                else:  # only clear if denied and don't clear blacklist status
                    await self.sql.execute("DELETE FROM pre_redditors WHERE redditor=$1", redditor)
                    deny_type = "Denied"
                    note = ""
                if isinstance(member, discord.Member):
                    if not self.bot.debug and context:
                        confirm = await context.prompt(
                            f"Would you like to kick {member.mention} from the server?{note}",
                            hidden=hidden,
                        )
                        if confirm:
                            try:
                                await member.kick(
                                    reason=f"{deny_type} by {actor.name}#{actor.discriminator} ({actor.id})"
                                )
                                await context.send(
                                    embed=generate_result_embed(
                                        f"Successfully kicked {member.mention} from the server!"
                                    ),
                                    hidden=hidden,
                                )
                            except Exception as error:
                                await context.send(
                                    embed=generate_result_embed(
                                        f"Failed to kick {member.mention} from the server!",
                                        result_type=EmbedType.error,
                                        contact_me=True,
                                    ),
                                    hidden=hidden,
                                )
                                self.log.exception(error)
                    await self.update_roles(member, add_roles=self.denied_role, remove_roles=self.unapproved_role)
                    member = member.id

                query_args = [member, actor.id, "deny"]
                await self.sql.execute("UPDATE redditmodhelper.users SET status=$1 WHERE user_id=$2", "denied", member)
                if approval_message:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type, channel_id, message_id) VALUES ($1, $2, $3, $4, $5)"
                    query_args += [approval_message.channel.id, approval_message.id]
                else:
                    query = "INSERT INTO approval_log (user_id, actor_id, action_type) VALUES ($1, $2, $3)"
                await self.sql.execute(query, *query_args)
        except Exception:
            message = f"Failed to deny <@{member.id if isinstance(member, discord.Member) else member}>."
            if context:
                await context.send(
                    embed=generate_result_embed(message, result_type=EmbedType.error, contact_me=True), hidden=hidden
                )
            else:
                await self.error_embed(self.approval_channel, message)

    async def check_existing_status(self, action, context, redditor):
        result = await self.sql.fetchval(
            "SELECT user_id FROM credential_store.user_verifications WHERE redditor=$1", redditor
        )
        if result:
            member_id = int(result)
            result = parse_sql(
                await self.sql.fetch("SELECT * FROM redditmodhelper.users WHERE user_id=$1", member_id), fetch_one=True
            )
            if result:
                if (result.status == "approved" and action == "denied") or (
                    result.status == "denied" and action == "approved"
                ):
                    actor, timestamp = await self.get_actor_for_member(member_id)
                    actor_str = (
                        actor.mention
                        if isinstance(actor, discord.Member)
                        else "grandfather"
                        if actor == 0
                        else f"<@{actor}>"
                    )
                    confirm = await context.prompt(
                        f"It appears that {actor_str} already {result.status} u/{redditor} on {timestamp}.\nDo you want to override?"
                    )
                    if confirm:
                        member = context.guild.get_member(member_id)
                        if member:
                            if action == "denied":
                                await self.deny_user(context, member, redditor, context.author)
                            elif action == "approved":
                                await self.approve_user(member, redditor, context.actor)
                        await self.sql.execute(
                            "UPDATE redditmodhelper.users SET status=$1 WHERE user_id=$2", action, member_id
                        )
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
            actor = self.bot.snoo_guild.get_member(result.actor_id).mention or result.actor_id
            timestamp = result.timestamp.astimezone().strftime(TIME_FORMAT)
            return status, actor, timestamp
        else:
            return [None] * 3

    async def execute_preemptive(self, member, redditor):
        preemptive_status, actor, timestamp = await self.check_pre_redditor(redditor)
        if preemptive_status == "denied":
            preemptive_status = "blacklisted"
            await self.deny_user(None, member, redditor, actor, preemptive=True)
        elif preemptive_status == "approved":
            preemptive_status = "whitelisted"
            await self.approve_user(redditor, member, actor, preemptive=True, send_embed=False)
        if all([preemptive_status, actor, timestamp]):
            await self.send_approval_request(member, redditor, preemptive_status)
            return True
        return False

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

    async def get_user_info(self, member_id):
        return parse_sql(
            await self.sql.fetch("SELECT * FROM redditmodhelper.users WHERE user_id=$1", member_id), fetch_one=True
        )

    async def insert_user(self, member, on_join=False):
        self.sql = self.bot.pool
        print('insert user')
        results = parse_sql(
            await self.sql.fetch(
                f"""INSERT INTO redditmodhelper.users (user_id, username, created_at, joined_at, first_joined_at)
                        VALUES ($1, $2, $3, $4, $4)
                        ON CONFLICT (user_id) DO UPDATE SET joined_at=excluded.joined_at
                        RETURNING *""", member.id, member.name, member.created_at, getattr(member, "join_at", None)), fetch_one=True)
        if on_join:
            await self.sql.execute('UPDATE redditmodhelper.users SET join_count=join_count+1 WHERE user_id=$1', member.id)
            results = parse_sql(await self.sql.fetch('select * from redditmodhelper.users WHERE user_id=$1', member.id), fetch_one=True)
        return results

    async def on_join(self, member):
        if member.bot:
            return
        self.sql = self.bot.pool
        result = await self.insert_user(member, on_join=True)
        await self.update_roles(member, add_roles=self.unverified_role)
        redditor = await self.get_redditor(member)
        if redditor:  # already verified
            await self.set_verified(member, update_status=result.status == "unverified")
            await self.dmz_channel.send(
                f"Welcome {member.mention}! You have already verified your account.\nNote: You may have to wait for approval before your able to access the rest of the server."
            )
            if not await self.execute_preemptive(member, redditor):
                if result.status == "approved":
                    await self.approve_user(member, redditor, None, previous=True)
                elif result.status == "denied":
                    await self.deny_user(None, member, redditor, previous=True)
                await self.send_approval_request(member, redditor)
        else:
            await self.dmz_channel.send(
                f"Welcome {member.mention}! Before you can access this server you need to verify your reddit account.\n\nUse the /verify slash command to get started."
            )

    def generate_verification_embed(self, member):
        embed = discord.Embed(
            title="Reddit Account Verification",
            description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
        )
        embed.add_field(
            name="Authenticate Here:", value=self.bot.credmgr_bot.redditApp.genAuthUrl(userVerification=str(member.id))
        )
        return embed

    async def pre_action_user(self, context, redditor, action, hidden):
        result = parse_sql(
            await self.sql.fetch("SELECT * FROM pre_redditors WHERE redditor=$1", redditor), fetch_one=True
        )
        if result:
            actor = context.guild.get_member(
                result.actor_id,
            )
            timestamp = result.timestamp.astimezone().strftime(TIME_FORMAT)
            confirm = await context.prompt(
                f"It appears that {actor.mention} already {'whitelisted' if result.status == 'approved' else 'blacklisted'} u/{result.redditor} on {timestamp}.\nDo you want to override?",
                hidden=hidden,
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
            await context.send(
                embed=generate_result_embed(
                    f"Successfully {'whitelisted' if action == 'approved' else 'blacklisted'} u/{redditor}!"
                ),
                hidden=hidden,
            )

    async def send_approval_request(self, member, redditor, preemptive_status=None):
        result = await self.get_user_info(member.id)
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
                    ""
                    if previous_action in ["approved", "whitelisted"]
                    else "Approving them will clear their blacklisted status."
                )
                embed = discord.Embed(
                    title=f"{'Already ' if previous_action in ['approved', 'denied'] else ''} {previous_action.title()} User Alert",
                    description=f"u/{redditor} was already {previous_action} by {actor} on {timestamp}{' and has been kicked' if previous_action in ['denied' 'blacklisted'] and await self.get_bot_config('auto_kick') else ''}.\n{note}",
                    color=discord.Color.orange(),
                )
            else:
                embed = discord.Embed(
                    title="New User",
                    description=f"{member.mention}",
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

    @command()
    @checks.authorized_roles()
    async def _on_join(self, context):
        member = context.guild.get_member(446113305614876682)
        await member.remove_roles(
            self.approved_role,
            self.grandfather_role,
            self.verified_role,
            self.unapproved_role,
            self.unverified_role,
            self.denied_role,
        )
        await self.on_join(member)

    @command()
    @checks.authorized_roles()
    async def _on_verify(self, context):
        member = context.guild.get_member(857055123078119474)
        await self.update_roles(
            member,
            add_roles=[self.unapproved_role, self.verified_role],
            remove_roles=[self.approved_role, self.grandfather_role, self.unverified_role, self.denied_role],
        )
        await self.sql.execute(
            "UPDATE redditmodhelper.users SET status='verified' WHERE user_id=$1", 857055123078119474
        )
        await self.send_approval_request(member, "Lil_SpazTest")

    @command()
    @checks.is_admin()
    async def adjuser(self, context: Context):
        for user in context.guild.members:
            redditor = await self.get_redditor(user)
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

    @command()
    @checks.authorized_roles()
    async def assignunv(self, context):
        for user in context.guild.members:
            await self.insert_user(user)
            redditor = await self.get_redditor(user)
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

    @cog_subcommand(
        base="blacklist",
        name="add",
        base_default_permission=False,
        options=[create_option("redditor", "Redditor to blacklist.", str, True)],
    )
    @permission(
        785198941535731715,
        generate_permissions(
            allowed_roles=[785203089001938974, 786230454286024775], allowed_users=[393801572858986496]
        ),
    )
    async def _blacklist_add(self, context, redditor):
        """Preemptively deny a redditor from accessing this server."""
        hidden = context.channel not in self.admin_category.channels
        await context.defer(hidden=hidden)
        user = await self.get_mod(context, redditor)
        await self.pre_action_user(context, user, "denied", hidden)

    @cog_subcommand(
        base="blacklist",
        name="remove",
        base_default_permission=False,
        options=[create_option("redditor", "Redditor to remove from the blacklist.", str, True)],
    )
    @permission(
        785198941535731715,
        generate_permissions(
            allowed_roles=[785203089001938974, 786230454286024775], allowed_users=[393801572858986496]
        ),
    )
    async def _blacklist_remove(self, context: InteractionContext, redditor):
        """Remove a redditor from the blacklist."""
        list_type = "blacklist"
        hidden = context.channel not in self.admin_category.channels
        await context.defer(hidden=hidden)
        user = await self.get_mod(context, redditor)
        status = "approved" if list_type == "whitelist" else "denied"
        result = parse_sql(
            await self.sql.fetch("SELECT * FROM pre_redditors WHERE redditor=$1 and status=$2", user, status),
            fetch_one=True,
        )
        if result:
            try:
                await self.sql.execute("DELETE FROM pre_redditors WHERE status=$1 AND redditor=$2", status, user)
                await context.send(embed=generate_result_embed(f"Removed u/{user} from {list_type}."), hidden=hidden)
            except Exception:
                await context.send(
                    embed=generate_result_embed(
                        f"Failed to remove u/{user} from {list_type}.", result_type=EmbedType.error, contact_me=True
                    ),
                    hidden=hidden,
                )
        else:
            await context.send(
                embed=generate_result_embed(
                    f"u/{user} is not currently {list_type}ed.",
                    result_type=EmbedType.warning,
                    title=f"Not {list_type.title()}ed",
                ),
                hidden=hidden,
            )
            return
        result = await self.sql.fetchval(
            "SELECT user_id FROM credential_store.user_verifications WHERE redditor=$1", user
        )
        if result:
            member = context.guild.get_member(int(result))
            if member:
                action = "approve" if status == "denied" else "deny"
                confirm = await context.prompt(
                    f"u/{user} is currently in the server {member.mention}. Would you like to {action} them?"
                )
                if confirm:
                    await self.action_user(context, member, action, send_embed=False, hidden=hidden)

    @command(name="deny")
    @checks.authorized_roles()
    async def _deny(self, context):
        await context.send("This command has been converted to a slash command: `/deny`", delete_after=10)

    @command(name="approve")
    @checks.authorized_roles()
    async def _approve(self, context):
        await context.send("This command has been converted to a slash command: `/approve`", delete_after=10)

    @command(name="verify")
    async def _verify(self, context):
        await context.send("This command has been converted to a slash command: `/verify`")

    @cog_slash()
    async def verify(self, context: InteractionContext):
        """Verify your account Reddit account."""
        await context.defer(hidden=True)
        author = context.author
        result = await self.insert_user(author)
        redditor = await self.get_redditor(author)
        confirm = True
        if redditor:
            await self.set_verified(author, update_status=result.status == "unverified")
            confirm = await context.prompt(
                f"It appears you have already verified your reddit account (u/{redditor}). Would you like to reverify?",
                hidden=True,
            )
        if confirm:
            embed = self.generate_verification_embed(author)
            components = [create_actionrow(create_button(style=ButtonStyle.blurple, custom_id="done", label="Done"))]
            message = await context.send(
                f"Press the `Done` button below after you have verified your reddit account using this link:",
                embed=embed,
                components=components,
                hidden=True,
            )
            await self.sql.execute(
                "INSERT INTO approval_messages (user_id, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                author.id,
                int(message["id"]),
            )

    async def set_verified(self, member, update_status=True):
        await self.update_roles(member, add_roles=self.verified_role, remove_roles=self.unverified_role)
        if update_status:
            await self.sql.execute("UPDATE redditmodhelper.users set status=$1 WHERE user_id=$2", "verified", member.id)


def setup(bot):
    bot.add_cog(Permissions(bot))
