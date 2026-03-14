#!/usr/bin/env python3
"""
Unite - Companion app for SiamsoProtocol: content creators, collectibles, and fan exchange.
Single-file app: local state, optional RPC client, CLI, and REST API for BasedCollectiveFans.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------

UNITE_VERSION = (1, 0)
UNITE_APP_NAME = "Unite"
UNITE_STATE_FILE = "unite_state.json"
UNITE_DEFAULT_RPC = "https://mainnet.base.org"
UNITE_CHAIN_ID = 8453
UNITE_CREATOR_PREFIX = "creator_"
UNITE_COLLECTIBLE_PREFIX = "col_"
UNITE_LISTING_PREFIX = "list_"
UNITE_OFFER_PREFIX = "offer_"
UNITE_MAX_CREATORS = 50_000
UNITE_MAX_COLLECTIBLES_PER_CREATOR = 2_000
UNITE_MAX_LISTINGS_PER_COLLECTIBLE = 500
UNITE_MAX_OFFERS_PER_COLLECTIBLE = 500
UNITE_DEFAULT_FEE_BPS = 250
UNITE_BPS_CAP = 2_500
UNITE_ROYALTY_BPS_CAP = 1_000
UNITE_SEED_BASE = 0x7E3F1A9C

# -----------------------------------------------------------------------------
# EXCEPTIONS
# -----------------------------------------------------------------------------


class UniteError(Exception):
    """Base exception for Unite app."""

    pass


class UniteNotFoundError(UniteError):
    def __init__(self, kind: str, id_: str) -> None:
        super().__init__(f"{kind} not found: {id_}")
        self.kind = kind
        self.id = id_


class UniteValidationError(UniteError):
    def __init__(self, field_name: str, message: str = "") -> None:
        super().__init__(message or f"Invalid field: {field_name}")
        self.field_name = field_name


class UniteAuthError(UniteError):
    def __init__(self, message: str = "Not authorized") -> None:
        super().__init__(message)
