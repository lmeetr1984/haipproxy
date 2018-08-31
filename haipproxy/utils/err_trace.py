from raven import Client

from ..config.settings import SENTRY_DSN

__all__ = ['client']

# Raven is a client for Sentry

client = Client(SENTRY_DSN)
