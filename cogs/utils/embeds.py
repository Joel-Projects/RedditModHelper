from discord import Color
from discord import Embed as Embed_
from discord.embeds import EmptyEmbed, _EmptyEmbed


class Embed(Embed_):
    # Because fuck you colour
    @property
    def color(self):
        return getattr(self, "_colour", EmptyEmbed)

    @color.setter
    def color(self, value):
        if isinstance(value, (Color, _EmptyEmbed)):
            self._colour = value
        elif isinstance(value, int):
            self._colour = Color(value=value)
        else:
            raise TypeError(
                "Expected discord.Colour, int, or Embed.Empty but received %s instead." % value.__class__.__name__
            )
