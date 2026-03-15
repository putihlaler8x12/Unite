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
            k: asdict(v)
            for k, v in self._state.creators.items()
        }
        collectibles = {
            k: asdict(v)
            for k, v in self._state.collectibles.items()
        }
        listings = {
            k: asdict(v)
            for k, v in self._state.listings.items()
        }
        offers = {
            k: asdict(v)
            for k, v in self._state.offers.items()
        }
        fan_follows = [asdict(f) for f in self._state.fan_follows]
        royalty_configs = {
            k: asdict(v)
            for k, v in self._state.royalty_configs.items()
        }
        collectible_balances = {
            f"{c}|{a}": amt
            for (c, a), amt in self._state.collectible_balances.items()
        }
        return {
            "creators": creators,
            "collectibles": collectibles,
            "listings": listings,
            "offers": offers,
            "fan_follows": fan_follows,
            "royalty_configs": royalty_configs,
            "collectible_balances": collectible_balances,
            "creator_by_address": self._state.creator_by_address,
            "next_creator_num": self._state.next_creator_num,
            "next_collectible_num": self._state.next_collectible_num,
            "next_listing_num": self._state.next_listing_num,
            "next_offer_num": self._state.next_offer_num,
        }

    def _deserialize(self, data: Dict[str, Any]) -> None:
        self._state.creators = {
            k: CreatorRecord(**v)
            for k, v in data.get("creators", {}).items()
        }
        self._state.collectibles = {
            k: CollectibleRecord(**v)
            for k, v in data.get("collectibles", {}).items()
        }
        self._state.listings = {
            k: ListingRecord(**v)
            for k, v in data.get("listings", {}).items()
        }
        self._state.offers = {
            k: OfferRecord(**v)
            for k, v in data.get("offers", {}).items()
        }
        self._state.fan_follows = [
            FanFollowRecord(**f)
            for f in data.get("fan_follows", [])
        ]
        self._state.royalty_configs = {
            k: RoyaltyConfigRecord(**v)
            for k, v in data.get("royalty_configs", {}).items()
        }
        cb = data.get("collectible_balances", {})
        self._state.collectible_balances = {
            (k.split("|")[0], k.split("|")[1]): v
            for k, v in cb.items()
        }
        self._state.creator_by_address = data.get("creator_by_address", {})
        self._state.next_creator_num = data.get("next_creator_num", 1)
        self._state.next_collectible_num = data.get("next_collectible_num", 1)
        self._state.next_listing_num = data.get("next_listing_num", 1)
        self._state.next_offer_num = data.get("next_offer_num", 1)

    @property
    def state(self) -> UniteState:
        return self._state

    def get_creator(self, creator_id: str) -> CreatorRecord:
        if creator_id not in self._state.creators:
            raise UniteNotFoundError("Creator", creator_id)
        return self._state.creators[creator_id]

    def get_collectible(self, collectible_id: str) -> CollectibleRecord:
        if collectible_id not in self._state.collectibles:
            raise UniteNotFoundError("Collectible", collectible_id)
        return self._state.collectibles[collectible_id]

    def get_listing(self, listing_id: str) -> ListingRecord:
        if listing_id not in self._state.listings:
            raise UniteNotFoundError("Listing", listing_id)
        return self._state.listings[listing_id]

    def get_offer(self, offer_id: str) -> OfferRecord:
        if offer_id not in self._state.offers:
            raise UniteNotFoundError("Offer", offer_id)
        return self._state.offers[offer_id]

    def balance_of(self, collectible_id: str, account: str) -> int:
        return self._state.collectible_balances.get((collectible_id, account), 0)

    def creator_id_for_address(self, account: str) -> Optional[str]:
        return self._state.creator_by_address.get(account)


# -----------------------------------------------------------------------------
# CORE OPERATIONS
# -----------------------------------------------------------------------------


class UniteApp:
    def __init__(self, store: UniteStore) -> None:
        self._store = store

    def register_creator(
        self,
        account: str,
        content_root: str,
        handle: str,
    ) -> CreatorRecord:
        if self._store.creator_id_for_address(account):
            raise UniteValidationError("account", "Already registered as creator")
        if self._store.state.next_creator_num > UNITE_MAX_CREATORS:
            raise UniteValidationError("next_creator_num", "Max creators reached")
        now = time.time()
        creator_id = f"{UNITE_CREATOR_PREFIX}{self._store.state.next_creator_num}"
        self._store.state.next_creator_num += 1
        rec = CreatorRecord(
            creator_id=creator_id,
            account=account,
            content_root=content_root,
            registered_at=now,
            updated_at=now,
            handle=handle,
            active=True,
        )
        self._store.state.creators[creator_id] = rec
        self._store.state.creator_by_address[account] = creator_id
        return rec
