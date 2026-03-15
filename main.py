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


class UniteStateError(UniteError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


# -----------------------------------------------------------------------------
# DATA STRUCTURES
# -----------------------------------------------------------------------------


@dataclass
class CreatorRecord:
    creator_id: str
    account: str
    content_root: str
    registered_at: float
    updated_at: float
    handle: str
    active: bool = True


@dataclass
class CollectibleRecord:
    collectible_id: str
    creator_id: str
    content_hash: str
    supply_cap: int
    total_minted: int
    minted_at: float
    frozen: bool = False


@dataclass
class ListingRecord:
    listing_id: str
    collectible_id: str
    seller: str
    amount: int
    price_wei: int
    created_at: float
    expires_at: float
    filled: bool = False


@dataclass
class OfferRecord:
    offer_id: str
    collectible_id: str
    bidder: str
    amount: int
    price_wei: int
    created_at: float
    expires_at: float
    filled: bool = False


@dataclass
class FanFollowRecord:
    creator_id: str
    fan: str
    followed_at: float


@dataclass
class RoyaltyConfigRecord:
    collectible_id: str
    recipient: str
    bps: int


@dataclass
class UniteState:
    creators: Dict[str, CreatorRecord] = field(default_factory=dict)
    collectibles: Dict[str, CollectibleRecord] = field(default_factory=dict)
    listings: Dict[str, ListingRecord] = field(default_factory=dict)
    offers: Dict[str, OfferRecord] = field(default_factory=dict)
    fan_follows: List[FanFollowRecord] = field(default_factory=list)
    royalty_configs: Dict[str, RoyaltyConfigRecord] = field(default_factory=dict)
    collectible_balances: Dict[Tuple[str, str], int] = field(default_factory=dict)
    creator_by_address: Dict[str, str] = field(default_factory=dict)
    next_creator_num: int = 1
    next_collectible_num: int = 1
    next_listing_num: int = 1
    next_offer_num: int = 1


# -----------------------------------------------------------------------------
# CONTENT HASH HELPERS
# -----------------------------------------------------------------------------


def content_hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def content_hash_str(payload: str) -> str:
    return content_hash_bytes(payload.encode("utf-8"))


def content_root_from_hashes(hashes: List[str]) -> str:
    if not hashes:
        return hashlib.sha256(b"").hexdigest()
    current = hashes[0]
    for h in hashes[1:]:
        combined = (current + h).encode("utf-8") if isinstance(current, str) else current + h
        if isinstance(combined, str):
            combined = combined.encode("utf-8")
        current = hashlib.sha256(combined).hexdigest()
    return current


# -----------------------------------------------------------------------------
# STATE STORE
# -----------------------------------------------------------------------------


class UniteStore:
    def __init__(self, state_path: Optional[Path] = None) -> None:
        self._path = state_path or Path(UNITE_STATE_FILE)
        self._state = UniteState()

    def load(self) -> None:
        if not self._path.exists():
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._deserialize(data)
        except (json.JSONDecodeError, KeyError) as e:
            raise UniteStateError(f"Failed to load state: {e}") from e

    def save(self) -> None:
        data = self._serialize()
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _serialize(self) -> Dict[str, Any]:
        creators = {
