import asyncio, discord, time, asyncpg, logging, hashlib, string
from discord.ext.commands import Context, Cog
from typing import Union
from .utils import checks
from .utils.commands import command
from .utils.command_cog import CommandCog
from .utils.checks import is_admin
from .utils.utils import parse_sql
from discord import Embed

log = logging.getLogger('root')

command_attrs = {'hidden': True}


class Permissions(CommandCog):
    '''
    A collection of Permission commands
    '''
    def __init__(self, bot):
        super().__init__(bot)
        self.approval_channel: discord.TextChannel = None
        self.dmz_channel: discord.TextChannel = None
        self.unverified_role: discord.Role = None
        self.verified_role: discord.Role = None

    async def cog_before_invoke(self, context):
        await self.set_references(context.bot.get_guild(785198941535731715))

    async def set_references(self, guild: discord.Guild):
        objects = {
            'unverified_role': await self.get_bot_config('unverified_role'),
            'verified_role': await self.get_bot_config('verified_role'),
            'dmz_channel': await self.get_bot_config('dmz_channel'),
            'approval_channel': await self.get_bot_config('approval_channel'),
        }
        getObject = lambda id: discord.utils.get(guild.roles + guild.channels, id=id)
        for key, value in objects.items():
            setattr(self, key, getObject(value))
    #
    @Cog.listener()
    async def on_member_join(self, member):
        await member.add_roles(self.unverified_role)
        self.sql = self.bot.pool
        redditor = await self.get_user(None, member)
        if not redditor:
            embed = discord.Embed(title='Reddit Account Verification', description='In order for me to verify your Reddit username, I need you to grant me **temporary** access:')
            verification = self.bot.credmgr.userVerification.create(str(member.id), self.bot.credmgr_bot.redditApp)
            embed.add_field(name='Authenticate Here:', value=self.bot.credmgr_bot.redditApp.genAuthUrl(userVerification=verification), inline=True)
            await self.dmz_channel.send(embed=embed)
            await self.dmz_channel.send(f'Welcome {member.mention}! Send `.done` after you have verified your reddit account using the above link.')
        else:
            print()
            # add approval here

    @command()
    async def verify(self, context):
        embed = discord.Embed(title='Reddit Account Verification', description='In order for me to verify your Reddit username, I need you to grant me **temporary** access:')
        verification = self.bot.credmgr.userVerification.create(str(context.author.id), self.bot.credmgr_bot.redditApp)
        embed.add_field(name='Authenticate Here:', value=self.bot.credmgr_bot.redditApp.genAuthUrl(userVerification=verification), inline=True)
        await context.author.send(embed=embed)
        await context.author.send(f'Send `.done` after you have verified your reddit account using the above link.')

    @command(aliases=['ra'])
    async def reauth(self, context: Context, *users: discord.Member):
        admin_roles = [discord.utils.get(context.guild.roles, id=role_id) for role_id in await self.get_bot_config('authorized_users')]
        authorized_users = [member for role in admin_roles for member in role.members]
        if users:
            if len(users) > 1:
                if context.author in authorized_users:
                    for member in users:
                        authorID = member.id
                        encodedAuthor = hashlib.sha256(str(authorID).encode('utf-8')).hexdigest()
                        embed = discord.Embed(title='Reddit Account Reverification', description='In order for me to reverify your Reddit username, I need you to grant me access again:')
                        embed.add_field(name='Reauthenticate Here:', value=self.reddit.auth.url(['identity'], self.reddit.config.custom['state'] + encodedAuthor), inline=True)
                        msg = await context.send(embed=embed)
                        msg2 = await context.send(f'{member.mention}, send `.redone` after you have verified your reddit account using the above link.')
                        context = await self.bot.get_context(msg)
                else:
                    await self.error_embed(context, 'Only admins can specify users.')
            elif users[0] != context.message.author:
                await self.error_embed(context, 'Only admins can specify other users.')
            return
        else:
            member = context.message.author
            authorID = member.id
            encodedAuthor = hashlib.sha256(str(authorID).encode('utf-8')).hexdigest()
            results = parse_sql(await self.sql.fetch('SELECT * FROM verified WHERE member_id=$1', authorID))
            currentAuthed = results[0].redditor
            embed = discord.Embed(title='Reauth Confirmation')
            confirm = await context.prompt(f'Are you sure you want to reauth your reddit account? Your currently authed account is: u/{currentAuthed}', embed=embed, delete_after=True, return_message=False)
            if confirm:
                embed = discord.Embed(title='Reddit Account Reverification', description='In order for me to reverify your Reddit username, I need you to grant me access again:')
                embed.add_field(name='Reauthenticate Here:', value=self.reddit.auth.url(['identity'], self.reddit.config.custom['state'] + encodedAuthor), inline=True)
                msg = await context.send(embed=embed)
                msg2 = await context.send(f'Send `.redone` after you have verified your reddit account using the above link.')
            else:
                embed = discord.Embed(title='Cancelled', color=discord.Color.orange())
                embed.set_footer(text=time.strftime('%B %d, %Y at %I:%M:%S %p %Z', time.localtime()))
                await context.send(embed=embed)

    @command(hidden=True)
    async def redone(self, context: Context, *users):
        if users:
            if await checks.check_guild_permissions(context, {'administrator': True}):
                for user in users:
                    redditor = await self.get_user(context, user)
                    if redditor:
                        personalChannel = await self.getPersonalChannel(context, user, create=True)
                        role = await self.getPersonalRole(context, user, create=True)
                        await self.update_perms(context, user, redditor, personalChannel, role, createNewChannels=True)
                        await self.successEmbed(context, f'Reauthenticated for {user.mention} successfully!')
            else:
                await self.error_embed(context, 'This command requires the administrator permission when acting on other users.')
        else:
            await self.set_references(context.guild)
            redditor = await self.get_user(context, context.author)
            if redditor:
                personalChannel = await self.getPersonalChannel(context, context.author, create=True)
                role = await self.getPersonalRole(context, context.author, create=True)
                await self.update_perms(context, context.author, redditor, personalChannel, role, createNewChannels=True)
                await self.successEmbed(context, f'Reauthenticated for {context.author.mention} successfully!')

    @command(aliases=['ump'])
    async def updatemyperms(self, context: Context, *users: discord.Member):
        if users:
            if await checks.check_guild_permissions(context, {'administrator': True}):
                for user in users:
                    redditor = await self.get_user(context, user)
                    if redditor:
                        personalChannel = await self.getPersonalChannel(context, user, create=True)
                        role = await self.getPersonalRole(context, user, create=True)
                        await self.update_perms(context, user, redditor, personalChannel, role, createNewChannels=True)
                        await self.successEmbed(context, f'Updated permission for {user.mention} successfully!')
            else:
                await self.error_embed(context, 'This command requires the administrator permission when acting on other users.')
        else:
            redditor = await self.get_user(context, context.author)
            if redditor:
                await self.update_perms(context, context.author, redditor, personalChannel, role, createNewChannels=True)

    @command(hidden=True)
    async def done(self, context: Context, *userid: int):
        if context.guild:
            if userid:
                user = discord.utils.get(context.guild.members, id=userid[0])
            else:
                user = context.author
        else:
            user = discord.utils.get(context.bot.get_guild(785198941535731715).members, id=context.author.id)
        redditor = await self.get_user(context, user)
        if redditor:
            await self.update_perms(context, user, redditor)
            await self.success_embed(context, f'Verified u/{redditor} successfully!')
            embed = Embed(title='Success', color=discord.Color.green(), description=f'Verification Complete! Please wait for approval.')
            embed.set_footer(text=time.strftime('%B %d, %Y at %I:%M:%S %p %Z', time.localtime()))
        else:
            await self.error_embed(context, 'Unable to verify your reddit account, please try authorizing with the link above again.')

    async def update_perms(self, context, author, redditor):
        await author.add_roles(self.verified_role)
        await author.remove_roles(self.unverified_role)
        redditor = await self.reddit.redditor(redditor)
        moderated_subreddits = await redditor.moderated()
        results = parse_sql(await self.sql.fetch('SELECT name, role_id FROM subreddits'))
        if results:
            roles = [result.role_id for result in results if result.name in moderated_subreddits]
            for role in roles:
                await author.add_roles(context.bot.get_guild(785198941535731715).get_role(role))

def setup(bot):
    bot.add_cog(Permissions(bot))
