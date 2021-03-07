import hashlib
import logging
import time
from datetime import datetime

import asyncprawcore
import discord
from asyncpg import UniqueViolationError
from discord.ext.commands import Cog, Context

from .utils import checks, db
from .utils import time as utime
from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.converters import NotFound, RedditorConverter, UserIDConverter
from .utils.utils import parse_sql


class Users(db.Table, table_name="users"):
    id = db.PrimaryKeyColumn()
    user_id = db.Column(db.Integer(big=True), index=True, unique=True)
    username = db.Column(db.String, nullable=False)
    created_at = db.Column(db.Datetime(timezone=True), nullable=False)
    joined_at = db.Column(db.Datetime(timezone=True), nullable=False)
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
    channel_id = db.Column(db.Integer(big=True), nullable=False)
    message_id = db.Column(db.Integer(big=True), nullable=False)
    actioned_at = db.Column(db.Datetime(timezone=True), default="NOW()")


class PreRedditors(db.Table, table_name="pre_redditors"):
    id = db.PrimaryKeyColumn()
    redditor = db.Column(db.String, nullable=False)
    actor_id = db.Column(db.Integer(big=True), index=True, nullable=False)
    status = db.Column(db.Status, nullable=False)
    timestamp = db.Column(db.Datetime(timezone=True), default="NOW()")


class Permissions(CommandCog, command_attrs={"hidden": True}):
    """
    A collection of Permission commands
    """

    def __init__(self, bot):
        super().__init__(bot)
        self.unverified_role: discord.Role = None
        self.verified_role: discord.Role = None
        self.approved_role: discord.Role = None
        self.dmz_channel: discord.TextChannel = None
        self.approval_channel: discord.TextChannel = None
        self.grandfather_role: discord.Role = None
        self.guild = self.bot.get_guild(785198941535731715)

    async def cog_before_invoke(self, context):
        await self.set_references(context.bot.get_guild(785198941535731715))

    async def set_references(self, guild: discord.Guild):
        objects = {
            "unverified_role": await self.get_bot_config("unverified_role"),
            "verified_role": await self.get_bot_config("verified_role"),
            "approved_role": await self.get_bot_config("approved_role"),
            "dmz_channel": await self.get_bot_config("dmz_channel"),
            "approval_channel": await self.get_bot_config("approval_channel"),
            "grandfather_role": await self.get_bot_config("grandfather_role"),
        }
        getObject = lambda id: discord.utils.get(guild.roles + guild.channels, id=id)
        for key, value in objects.items():
            setattr(self, key, getObject(value))

    async def send_approval_request(
        self, user_id, member, redditor, verified_message=None, note=""
    ):
        redditor = await self.reddit.redditor(redditor, fetch=True)
        time_format = "%B %d, %Y at %I:%M:%S %p %Z"
        channel = self.bot.get_channel(await self.get_bot_config("approval_channel"))
        if note:
            note = f"\n__***{note}***__"
        embed = discord.Embed(
            title="New User",
            description=f"Send `.approve {user_id}` or `.deny {user_id}` to approve/deny user.\n***Note: If you deny this user they will be immediately kicked.***{note}",
            url=verified_message.jump_link if verified_message else None,
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
            value=time.strftime(time_format, time.localtime(redditor.created_utc))
            + f"\n({utime.human_timedelta(datetime.utcfromtimestamp(redditor.created_utc).astimezone())})",
        )
        embed.add_field(
            name="Joined Discord",
            value=member.created_at.astimezone().strftime(time_format)
            + f"\n({utime.human_timedelta(member.created_at.astimezone())})",
        )
        embed.add_field(
            name="Joined Server",
            value=member.joined_at.astimezone().strftime(time_format)
            + f"\n({utime.human_timedelta(member.joined_at.astimezone())})",
        )
        embed.set_footer(text=time.strftime(time_format, time.localtime()))
        _, _, subCount, subreddits, subscribers, _ = await self.get_and_calculate_subs(
            str(redditor)
        )
        embed.add_field(name="Subreddit Count", value=f"{subCount:,}")
        embed.add_field(name="Subscriber Count", value=f"{subscribers:,}")
        valueString = (
            "\n".join(
                [
                    f"{subRank}. {subreddit[0]}: {subreddit[1]:,}"
                    for subRank, subreddit in enumerate(subreddits[:20], 1)
                ]
            )
            if subreddits
            else "This user does not moderate any subreddits."
        )
        embed.add_field(name="Top 20 Subreddits", value=valueString, inline=False)
        return await channel.send(embed=embed)

    async def send_automatic_kick(
        self, user_id, member, redditor, verified_message=None
    ):
        redditor = await self.reddit.redditor(redditor, fetch=True)
        time_format = "%B %d, %Y at %I:%M:%S %p %Z"
        channel = self.bot.get_channel(await self.get_bot_config("approval_channel"))
        embed = discord.Embed(
            title="New User",
            description=f"Send `.approve {user_id}` or `.deny {user_id}` to approve/deny user.\n***Note: If you deny this user they will be immediately kicked.***",
            url=verified_message.jump_link if verified_message else None,
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
            value=time.strftime(time_format, time.localtime(redditor.created_utc))
            + f"\n({utime.human_timedelta(datetime.utcfromtimestamp(redditor.created_utc).astimezone())})",
        )
        embed.add_field(
            name="Joined Discord",
            value=member.created_at.astimezone().strftime(time_format)
            + f"\n({utime.human_timedelta(member.created_at.astimezone())})",
        )
        embed.add_field(
            name="Joined Server",
            value=member.joined_at.astimezone().strftime(time_format)
            + f"\n({utime.human_timedelta(member.joined_at.astimezone())})",
        )
        embed.set_footer(text=time.strftime(time_format, time.localtime()))
        _, _, subCount, subreddits, subscribers, _ = await self.get_and_calculate_subs(
            str(redditor)
        )
        embed.add_field(name="Subreddit Count", value=f"{subCount:,}")
        embed.add_field(name="Subscriber Count", value=f"{subscribers:,}")
        valueString = (
            "\n".join(
                [
                    f"{subRank}. {subreddit[0]}: {subreddit[1]:,}"
                    for subRank, subreddit in enumerate(subreddits[:20], 1)
                ]
            )
            if subreddits
            else "This user does not moderate any subreddits."
        )
        embed.add_field(name="Top 20 Subreddits", value=valueString, inline=False)
        return await channel.send(embed=embed)

    @Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await member.add_roles(self.unverified_role)
        self.sql = self.bot.pool
        try:
            await Users.insert(
                user_id=member.id,
                username=member.name,
                created_at=member.created_at,
                joined_at=member.joined_at,
            )
        except UniqueViolationError:
            pass
        redditor = await self.get_redditor(None, member)
        if not redditor:
            embed = discord.Embed(
                title="Reddit Account Verification",
                description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
            )
            verification = self.bot.credmgr.userVerification.create(
                str(member.id), self.bot.credmgr_bot.redditApp
            )
            embed.add_field(
                name="Authenticate Here:",
                value=self.bot.credmgr_bot.redditApp.genAuthUrl(
                    userVerification=verification
                ),
                inline=True,
            )
            link_message = await self.dmz_channel.send(embed=embed)
            welcome_message = await self.dmz_channel.send(
                f"Welcome {member.mention}! Send `.done` after you have verified your reddit account using the above link."
            )
            await self.sql.execute(
                "UPDATE users SET link_message_id=$1, welcome_message_id=$2 WHERE user_id=$3",
                link_message.id,
                welcome_message.id,
                member.id,
            )
        else:
            message = await self.success_embed(
                self.dmz_channel,
                f"Verified u/{redditor} successfully!\nNote: you will have to wait for approval before you are allowed to access the server.",
            )
            welcome_message = await self.dmz_channel.send(
                f"Welcome {member.mention}! You have already been verified, please wait for manual approval."
            )
            await self.sql.execute(
                "UPDATE users SET link_message_id=$1, welcome_message_id=$2 WHERE user_id=$3",
                message.id,
                welcome_message.id,
                member.id,
            )
            result = await self.get_member(member.guild, member.id)
            if result:
                if result.status in ["approved", "denied"]:
                    results = parse_sql(
                        await self.sql.fetch(
                            "SELECT * FROM approval_log WHERE user_id=$1 ORDER BY actioned_at DESC LIMIT 1",
                            member.id,
                        )
                    )
                    if results:
                        actor = await self.get_member(
                            member.guild, results[0].actor_id, return_member=True
                        )
                        await self.send_approval_request(
                            result.id,
                            member,
                            redditor,
                            note=f"This user was {result.status} by {actor.name}#{actor.discriminator} ({actor.id})",
                        )
                else:
                    await self.send_approval_request(
                        result.id,
                        member,
                        redditor,
                        note=f"This user was approved by {actor.name}#{actor.discriminator} ({actor.id})",
                    )

            else:
                self.log.error("This should not be possible")

    @command(hidden=True)
    @checks.authorized_roles()
    async def _on_join(self, context, *members: discord.Member):
        for member in members:
            await member.add_roles(self.unverified_role)
            self.sql = self.bot.pool
            try:
                await Users.insert(
                    user_id=member.id,
                    username=member.name,
                    created_at=member.created_at,
                    joined_at=member.joined_at,
                )
            except UniqueViolationError:
                pass
            redditor = await self.get_redditor(None, member)
            if not redditor:
                embed = discord.Embed(
                    title="Reddit Account Verification",
                    description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
                )
                verification = self.bot.credmgr.userVerification.create(
                    str(member.id), self.bot.credmgr_bot.redditApp
                )
                embed.add_field(
                    name="Authenticate Here:",
                    value=self.bot.credmgr_bot.redditApp.genAuthUrl(
                        userVerification=verification
                    ),
                    inline=True,
                )
                link_message = await self.dmz_channel.send(embed=embed)
                welcome_message = await self.dmz_channel.send(
                    f"Welcome {member.mention}! Send `.done` after you have verified your reddit account using the above link."
                )
                await self.sql.execute(
                    "UPDATE users SET link_message_id=$1, welcome_message_id=$2 WHERE user_id=$3",
                    link_message.id,
                    welcome_message.id,
                    member.id,
                )
            else:
                message = await self.success_embed(
                    context,
                    f"Verified u/{redditor} successfully!\nNote: you will have to wait for approval before you are allowed to access the server.",
                )
                welcome_message = await self.dmz_channel.send(
                    f"Welcome {member.mention}! You have already been verified, please wait for manual approval."
                )
                await self.sql.execute(
                    "UPDATE users SET link_message_id=$1, welcome_message_id=$2 WHERE user_id=$3",
                    message.id,
                    welcome_message.id,
                    member.id,
                )
                result = await self.get_member(member.guild, member.id)
                if result:
                    if result.status in ["approved", "denied"]:
                        results = parse_sql(
                            await self.sql.fetch(
                                "SELECT * FROM approval_log WHERE user_id=$1 ORDER BY actioned_at DESC LIMIT 1",
                                member.id,
                            )
                        )
                        if results:
                            actor = await self.get_member(
                                member.guild, results[0].actor_id, return_member=True
                            )
                            await self.send_approval_request(
                                result.id,
                                member,
                                redditor,
                                note=f"This user was {result.status} by {actor.name}#{actor.discriminator} ({actor.id})",
                            )
                    else:
                        await self.send_approval_request(
                            result.id,
                            member,
                            redditor,
                            note=f"This user was approved by {actor.name}#{actor.discriminator} ({actor.id})",
                        )

                else:
                    self.log.error("This should not be possible")

    async def get_member(self, guild, member_id, return_member=False):
        if member_id > 2147483647:
            query = "SELECT * FROM users WHERE user_id=$1"
        else:
            query = "SELECT * FROM users WHERE id=$1"
        results = parse_sql(await self.sql.fetch(query, member_id))
        if results:
            result = results[0]
            return (
                guild.get_member(
                    result.user_id,
                )
                if return_member
                else result
            )
        else:
            return None

    @command()
    @checks.authorized_roles()
    async def assignunv(self, context):
        for user in context.guild.members:
            try:
                await Users.insert(
                    user_id=user.id,
                    username=user.name,
                    created_at=user.created_at,
                    joined_at=user.joined_at,
                )
            except UniqueViolationError:
                pass
            redditor = await self.get_redditor(context, user)
            if self.approved_role not in user.roles:
                if redditor:
                    await user.add_roles(self.verified_role, self.approved_role)
                    await user.remove_roles(self.grandfather_role, self.unverified_role)
                    self.log.info(f'Added approved_role removed grandfather_role role to {user}')
                else:
                    await user.add_roles(self.unverified_role, self.grandfather_role)
                    await user.remove_roles(self.approved_role, self.verified_role)
                    self.log.info(f'Added grandfather_role role to {user}')
            else:
                if redditor:
                    await user.add_roles(self.verified_role, self.approved_role)
                    await user.remove_roles(self.unverified_role, self.grandfather_role)
                else:
                    await user.add_roles(self.unverified_role, self.grandfather_role)
                self.log.info(f'{user} already approved')

    @command()
    @checks.authorized_roles()
    async def approve(self, context, *user_ids: UserIDConverter):
        """Approve a user to access this server.

        Params:

        user_ids: One or more users to approve. This can be the number in the approval embed or a discord user id or a user mention.

        Examples:

        `.approve 1` approves user 1. This is the number provided in the approval embed.
        `.approve 1 2` same as previous but also approves user 2.
        `.approve 393801572858986496` approves user with discord user id 393801572858986496.
        `.approve @N8theGr8` this approves N8theGr8.
        """
        users = set(user_ids)
        for user in users:
            if isinstance(user, NotFound):
                await self.error_embed(
                    context, f"Could not find user matching {user.arg}"
                )
            else:
                await self.action_user(context, user, "approve")

    @command()
    @checks.authorized_roles()
    async def deny(self, context, *user_ids: UserIDConverter):
        """Deny a user to access this server.

        Params:

        user_ids: One or more users to deny. This can be the number in the approval embed or a discord user id or a user mention.

        Examples:

        `.deny 1` denies user 1. This is the number provided in the approval embed.
        `.deny 1 2` same as previous but also denies user 2.
        `.deny 393801572858986496` denies user with discord user id 393801572858986496.
        `.deny @N8theGr8` this denies N8theGr8.
        """
        users = set(user_ids)
        for user in users:
            if isinstance(user, NotFound):
                await self.error_embed(
                    context, f"Could not find user matching {user.arg}"
                )
            else:
                await self.action_user(context, user, "deny")

    @command()
    @checks.authorized_roles()
    async def preapprove(self, context, *usernames: RedditorConverter):
        """Pre-approve a user to access this server.

        Params:

        usernames: One or more redditors to pre-approve. This is the redditor's username case insensitive.

        Examples:

        `.preapprove spez` pre-approves user spez.
        `.preapprove spez N8theGr8` same as previous but also approves N8theGr8.
        """
        for user in usernames:
            await self.pre_action_user(context, user, "approved")

    async def pre_action_user(self, context, redditor, action):
        self.bot.credmgr.userVerification()
        await self.sql.execute(
            "INSERT INTO pre_redditors (redditor, actor_id, status) VALUES ($1, $2, $3) ON CONFLICT (redditor) DO UPDATE SET status=excluded.status, timestamp=NOW()",
            redditor,
            context.author.id,
            action,
        )

    @command()
    @checks.authorized_roles()
    async def predeny(self, context, *usernames: RedditorConverter):
        """Pre-deny a user to access this server.

        Params:

        usernames: One or more redditors to pre-deny. This is the redditor's username case insensitive.

        Examples:

        `.predeny spez` pre-denies user spez.
        `.predeny spez N8theGr8` same as previous but also denies N8theGr8.
        """
        for user in usernames:
            await self.pre_action_user(context, user, "denied")

    async def action_user(self, context, member, action):
        if action in ["approve", "deny"]:
            if action == "approve":
                await member.add_roles(self.approved_role)
                await self.update_perms(member)
                await self.success_embed(
                    context, f"Successfully approved {member.mention}!"
                )
            elif action == "deny":
                await member.kick(
                    reason=f"Denied by {context.author.name}#{context.author.discriminator} ({context.author.id})"
                )
                await self.success_embed(
                    context, f"Successfully denied and kicked {member.mention}!"
                )
            await self.sql.execute(
                "UPDATE users SET status=$1 WHERE user_id=$2",
                "approved" if action == "approve" else "denied",
                member.id,
            )
            await self.sql.execute(
                "INSERT INTO approval_log (user_id, actor_id, action_type, channel_id, message_id) VALUES ($1, $2, $3, $4, $5)",
                member.id,
                context.author.id,
                action,
                context.channel.id,
                context.message.id,
            )

    @command()
    async def verify(self, context, *users: discord.Member):
        """Manually verify your account Reddit account."""
        users = await self.check_multiple_auth(context, "users", users, context.author)
        if not users:
            users = [context.author]
        for member in users:
            try:
                await Users.insert(
                    user_id=member.id,
                    username=member.name,
                    created_at=member.created_at,
                    joined_at=member.joined_at,
                )
            except UniqueViolationError:
                pass
            embed = discord.Embed(
                title="Reddit Account Verification",
                description="In order for me to verify your Reddit username, I need you to grant me **temporary** access:",
            )
            verification = self.bot.credmgr.userVerification.create(
                str(member.id), self.bot.credmgr_bot.redditApp
            )
            embed.add_field(
                name="Authenticate Here:",
                value=self.bot.credmgr_bot.redditApp.genAuthUrl(
                    userVerification=verification
                ),
                inline=True,
            )
            await member.send(embed=embed)
            await member.send(
                f"Send `.done {member.id}` after you have verified your reddit account using the above link."
            )

    @command(aliases=["ump"])
    async def updatemyperms(self, context: Context, *users: discord.Member):
        if users:
            if await checks.check_guild_permissions(context, {"administrator": True}):
                for user in users:
                    redditor = await self.get_redditor(context, user)
                    if redditor:
                        personalChannel = await self.getPersonalChannel(
                            context, user, create=True
                        )
                        role = await self.getPersonalRole(context, user, create=True)
                        await self.update_perms(
                            context,
                            user,
                            redditor,
                            personalChannel,
                            role,
                            createNewChannels=True,
                        )
                        await self.success_embed(
                            context,
                            f"Updated permission for {user.mention} successfully!",
                        )
            else:
                await self.error_embed(
                    context,
                    "This command requires the administrator permission when acting on other users.",
                )
        else:
            redditor = await self.get_redditor(context, context.author)
            if redditor:
                await self.update_perms(
                    context,
                    context.author,
                    redditor,
                    personalChannel,
                    role,
                    createNewChannels=True,
                )

    @command(hidden=True)
    async def done(self, context: Context, *userid: int):
        if context.guild:
            if userid:
                user = discord.utils.get(context.guild.members, id=userid[0])
            else:
                user = context.author
        else:
            user = discord.utils.get(self.guild.members, id=context.author.id)
        if self.grandfather_role in context.author.roles:
            await self.action_user(context, user, "approve")
            return
        redditor = await self.get_redditor(context, user)
        if redditor:
            # await self.check_pre_redditor(context, redditor)
            await self.sql.execute(
                "UPDATE users set status=$1 WHERE user_id=$2", "verified", user.id
            )
            results = parse_sql(
                await self.sql.fetch(
                    "SELECT id, link_message_id, welcome_message_id FROM users WHERE user_id=$1",
                    user.id,
                )
            )
            if results:
                result = results[0]
                messages_to_delete = [
                    self.dmz_channel.get_partial_message(getattr(result, attr))
                    for attr in ["link_message_id", "welcome_message_id"]
                    if getattr(result, attr)
                ]
                for message in messages_to_delete:
                    try:
                        await message.delete()
                    except Exception:
                        pass
                await self.success_embed(
                    context,
                    f"Verified u/{redditor} successfully!\nNote: you will have to wait for approval before you are allowed to access the server.",
                )
                await self.send_approval_request(result.id, user, redditor)
            else:
                await self.error_embed(
                    context,
                    "I was unable to verify your reddit account, please try authorizing with the link above again.",
                )
        else:
            await self.error_embed(
                context,
                "I was unable to verify your reddit account, please try authorizing with the link above again.",
            )

    async def update_perms(self, user):
        await user.add_roles(self.verified_role)
        await user.remove_roles(self.unverified_role)
        redditor = await self.reddit.redditor(await self.get_redditor(None, user))
        moderated_subreddits = await redditor.moderated()
        results = parse_sql(
            await self.sql.fetch("SELECT name, role_id FROM subreddits")
        )
        if results:
            roles = [
                result.role_id
                for result in results
                if result.name in moderated_subreddits
            ]
            for role in roles:
                await user.add_roles(self.guild.get_role(role))

    async def check_pre_redditor(self, context, user):
        try:
            redditor = await self.reddit.redditor(user, fetch=True)
        except asyncprawcore.NotFound:
            await self.error_embed(
                context,
                f"u/{user} does not exist or they deleted their account or have been suspended.",
            )
        else:
            return parse_sql(
                await self.sql.fetch(
                    "SELECT * FROM pre_redditors WHERE redditor=$1", redditor
                )
            )


def setup(bot):
    bot.add_cog(Permissions(bot))
