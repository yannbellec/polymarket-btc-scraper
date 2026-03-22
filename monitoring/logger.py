"""Logger structuré avec rich."""
import logging
from rich.logging import RichHandler
from config.settings import LOG_LEVEL

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(message)s",
    datefmt="[%H:%M:%S]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)
log = logging.getLogger("btc_scraper")
