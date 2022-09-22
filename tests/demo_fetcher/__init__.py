import pkg_resources

from tests.demo_fetcher.demo_fetcher import DemoFetcher


async def create_instance(**kwargs):
    """create fetcher instance and start session
    Returns:
        [QuotesFetcher]: the fetcher
    """
    fetcher = DemoFetcher()
    await fetcher.create_instance(**kwargs)
    return fetcher


__all__ = ["create_instance"]
