# WIP
from typing import Any
import pendulum
from pydantic import Field, SecretStr

from prefect.blocks.core import Block


class MyCustomBlockType(Block):
    # TODO -- Change description
    """
    A block that represents a secret api key paired with a URL.
    The value stored in the api_key attribute will be obfuscated when
    this block is logged or shown in the UI.
    Attributes:
        value: A string value that should be kept secret.
        url: str = A string value representing the URL associated with the API key.
        api_key: A string value that should be kept secret.
    Example:
        ```python
        from prefect.blocks.system import Secret
        custom_block = MyCustomBlockType.load("BLOCK_NAME")
        # Access the stored secret
        secret_block.get()
        ```
    """

    # TODO
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5uUmyGBjRejYuGTWbTxz6E/3003e1829293718b3a5d2e909643a331/image8.png?h=250"
    _documentation_url = "https://docs.prefect.io/api-ref/prefect/blocks/system/#prefect.blocks.system.Secret"

    url: str = Field(default=..., description="A URL")

    api_key: SecretStr = Field(
        default=..., description="A string value that should be kept secret."
    )

    # Add Custom Methods

    def get_dict(self):
        return {"url": str(self.url), "api_key": self.api_key.get_secret_value()}

    def get_url(self):
        return str(self.url)

    def get_api_key(self):
        return self.api_key.get_secret_value()
