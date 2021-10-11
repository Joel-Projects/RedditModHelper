from BotUtils import BotServices

import config

__version__ = "1.1.0"
bot_name = config.bot_name
description = "Hello! I am a bot written by Lil_SpazJoekp"

services = BotServices(bot_name)
log = services.logger()
