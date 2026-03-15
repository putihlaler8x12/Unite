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

    def update_creator_content(self, creator_id: str, account: str, new_content_root: str) -> CreatorRecord:
        rec = self._store.get_creator(creator_id)
        if rec.account != account:
            raise UniteAuthError("Not the creator account")
        if not rec.active:
            raise UniteValidationError("creator", "Creator inactive")
        rec.content_root = new_content_root
        rec.updated_at = time.time()
        return rec

    def mint_collectible(
        self,
        creator_id: str,
        account: str,
        content_hash: str,
        supply_cap: int,
        to: str,
    ) -> CollectibleRecord:
        creator = self._store.get_creator(creator_id)
        if creator.account != account:
            raise UniteAuthError("Not the creator account")
        if not creator.active:
            raise UniteValidationError("creator", "Creator inactive")
        if supply_cap < 1:
            raise UniteValidationError("supply_cap", "Must be >= 1")
        col_id = f"{UNITE_COLLECTIBLE_PREFIX}{self._store.state.next_collectible_num}"
        self._store.state.next_collectible_num += 1
        now = time.time()
        rec = CollectibleRecord(
            collectible_id=col_id,
            creator_id=creator_id,
            content_hash=content_hash,
            supply_cap=supply_cap,
            total_minted=1,
            minted_at=now,
            frozen=False,
        )
        self._store.state.collectibles[col_id] = rec
        key = (col_id, to)
        self._store.state.collectible_balances[key] = self._store.state.collectible_balances.get(key, 0) + 1
        return rec

    def mint_collectible_batch(
        self,
        creator_id: str,
        account: str,
        content_hash: str,
        supply_cap: int,
        recipients: List[str],
    ) -> CollectibleRecord:
        creator = self._store.get_creator(creator_id)
        if creator.account != account:
            raise UniteAuthError("Not the creator account")
        if not creator.active:
            raise UniteValidationError("creator", "Creator inactive")
        if supply_cap < len(recipients):
            raise UniteValidationError("supply_cap", "Supply cap less than recipients")
        col_id = f"{UNITE_COLLECTIBLE_PREFIX}{self._store.state.next_collectible_num}"
        self._store.state.next_collectible_num += 1
        now = time.time()
        rec = CollectibleRecord(
            collectible_id=col_id,
            creator_id=creator_id,
            content_hash=content_hash,
            supply_cap=supply_cap,
            total_minted=len(recipients),
            minted_at=now,
            frozen=False,
        )
        self._store.state.collectibles[col_id] = rec
        for to in recipients:
            key = (col_id, to)
            self._store.state.collectible_balances[key] = self._store.state.collectible_balances.get(key, 0) + 1
        return rec

    def transfer_collectible(
        self,
        collectible_id: str,
        from_account: str,
        to: str,
        amount: int,
    ) -> None:
        if amount <= 0:
            raise UniteValidationError("amount", "Must be positive")
        bal = self._store.balance_of(collectible_id, from_account)
        if bal < amount:
            raise UniteValidationError("balance", "Insufficient balance")
        key_from = (collectible_id, from_account)
        key_to = (collectible_id, to)
        self._store.state.collectible_balances[key_from] = bal - amount
        self._store.state.collectible_balances[key_to] = self._store.state.collectible_balances.get(key_to, 0) + amount

    def follow(self, creator_id: str, fan: str) -> None:
        self._store.get_creator(creator_id)
        self._store.state.fan_follows.append(
            FanFollowRecord(creator_id=creator_id, fan=fan, followed_at=time.time())
        )

    def unfollow(self, creator_id: str, fan: str) -> None:
        self._store.state.fan_follows = [
            f for f in self._store.state.fan_follows
            if not (f.creator_id == creator_id and f.fan == fan)
        ]

    def is_follower(self, creator_id: str, fan: str) -> bool:
        return any(
            f.creator_id == creator_id and f.fan == fan
            for f in self._store.state.fan_follows
        )

    def create_listing(
        self,
        collectible_id: str,
        seller: str,
        amount: int,
        price_wei: int,
        duration_seconds: float,
    ) -> ListingRecord:
        if amount <= 0 or price_wei <= 0:
            raise UniteValidationError("amount/price", "Must be positive")
        bal = self._store.balance_of(collectible_id, seller)
        if bal < amount:
            raise UniteValidationError("balance", "Insufficient balance")
        self._store.get_collectible(collectible_id)
        now = time.time()
        listing_id = f"{UNITE_LISTING_PREFIX}{self._store.state.next_listing_num}"
        self._store.state.next_listing_num += 1
        rec = ListingRecord(
            listing_id=listing_id,
            collectible_id=collectible_id,
            seller=seller,
            amount=amount,
            price_wei=price_wei,
            created_at=now,
            expires_at=now + duration_seconds,
            filled=False,
        )
        self._store.state.listings[listing_id] = rec
        key = (collectible_id, seller)
        self._store.state.collectible_balances[key] = self._store.state.collectible_balances.get(key, 0) - amount
        return rec

    def fill_listing(
        self,
        listing_id: str,
        buyer: str,
        amount: int,
        value_wei: int,
    ) -> None:
        listing = self._store.get_listing(listing_id)
        if listing.filled:
            raise UniteValidationError("listing", "Listing already filled")
        if time.time() > listing.expires_at:
            raise UniteValidationError("listing", "Listing expired")
        if amount <= 0 or amount > listing.amount:
            raise UniteValidationError("amount", "Invalid amount")
        total_wei = listing.price_wei * amount
        if value_wei < total_wei:
            raise UniteValidationError("value", "Insufficient value")
        listing.amount -= amount
        if listing.amount == 0:
            listing.filled = True
        key_buyer = (listing.collectible_id, buyer)
        self._store.state.collectible_balances[key_buyer] = (
            self._store.state.collectible_balances.get(key_buyer, 0) + amount
        )

    def cancel_listing(self, listing_id: str, seller: str) -> None:
        listing = self._store.get_listing(listing_id)
        if listing.seller != seller:
            raise UniteAuthError("Not the seller")
        if listing.filled:
            raise UniteValidationError("listing", "Already filled")
        amt = listing.amount
        listing.amount = 0
        listing.filled = True
        key = (listing.collectible_id, seller)
        self._store.state.collectible_balances[key] = self._store.state.collectible_balances.get(key, 0) + amt

    def place_offer(
        self,
        collectible_id: str,
        bidder: str,
        amount: int,
        price_wei: int,
        duration_seconds: float,
    ) -> OfferRecord:
        if amount <= 0 or price_wei <= 0:
            raise UniteValidationError("amount/price", "Must be positive")
        self._store.get_collectible(collectible_id)
        now = time.time()
        offer_id = f"{UNITE_OFFER_PREFIX}{self._store.state.next_offer_num}"
        self._store.state.next_offer_num += 1
        rec = OfferRecord(
            offer_id=offer_id,
            collectible_id=collectible_id,
            bidder=bidder,
            amount=amount,
            price_wei=price_wei,
            created_at=now,
            expires_at=now + duration_seconds,
            filled=False,
        )
        self._store.state.offers[offer_id] = rec
        return rec

    def accept_offer(
        self,
        offer_id: str,
