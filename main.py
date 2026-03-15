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
        seller: str,
        amount: int,
    ) -> None:
        offer = self._store.get_offer(offer_id)
        if offer.filled:
            raise UniteValidationError("offer", "Offer already filled")
        if time.time() > offer.expires_at:
            raise UniteValidationError("offer", "Offer expired")
        if amount <= 0 or amount > offer.amount:
            raise UniteValidationError("amount", "Invalid amount")
        bal = self._store.balance_of(offer.collectible_id, seller)
        if bal < amount:
            raise UniteValidationError("balance", "Insufficient balance")
        offer.amount -= amount
        if offer.amount == 0:
            offer.filled = True
        key_seller = (offer.collectible_id, seller)
        key_bidder = (offer.collectible_id, offer.bidder)
        self._store.state.collectible_balances[key_seller] = (
            self._store.state.collectible_balances.get(key_seller, 0) - amount
        )
        self._store.state.collectible_balances[key_bidder] = (
            self._store.state.collectible_balances.get(key_bidder, 0) + amount
        )

    def cancel_offer(self, offer_id: str, bidder: str) -> None:
        offer = self._store.get_offer(offer_id)
        if offer.bidder != bidder:
            raise UniteAuthError("Not the bidder")
        if offer.filled:
            raise UniteValidationError("offer", "Already filled")
        offer.amount = 0
        offer.filled = True

    def set_royalty(self, collectible_id: str, creator_account: str, recipient: str, bps: int) -> None:
        col = self._store.get_collectible(collectible_id)
        creator = self._store.get_creator(col.creator_id)
        if creator.account != creator_account:
            raise UniteAuthError("Not the creator")
        if bps > UNITE_ROYALTY_BPS_CAP:
            raise UniteValidationError("bps", "Royalty bps exceeds cap")
        self._store.state.royalty_configs[collectible_id] = RoyaltyConfigRecord(
            collectible_id=collectible_id,
            recipient=recipient,
            bps=bps,
        )


# -----------------------------------------------------------------------------
# QUERIES AND PAGINATION
# -----------------------------------------------------------------------------


def list_creators(store: UniteStore, offset: int = 0, limit: int = 50) -> List[CreatorRecord]:
    ids = sorted(store.state.creators.keys(), key=lambda x: store.state.creators[x].registered_at)
    return [store.state.creators[i] for i in ids[offset : offset + limit]]


def list_collectibles(store: UniteStore, offset: int = 0, limit: int = 50) -> List[CollectibleRecord]:
    ids = sorted(store.state.collectibles.keys(), key=lambda x: store.state.collectibles[x].minted_at)
    return [store.state.collectibles[i] for i in ids[offset : offset + limit]]


def list_collectibles_by_creator(store: UniteStore, creator_id: str) -> List[CollectibleRecord]:
    return [
        c for c in store.state.collectibles.values()
        if c.creator_id == creator_id
    ]


def list_active_listings(store: UniteStore, collectible_id: Optional[str] = None) -> List[ListingRecord]:
    now = time.time()
    out = [
        l for l in store.state.listings.values()
        if not l.filled and l.amount > 0 and l.expires_at > now
    ]
    if collectible_id:
        out = [l for l in out if l.collectible_id == collectible_id]
    return sorted(out, key=lambda x: x.created_at)


def list_active_offers(store: UniteStore, collectible_id: Optional[str] = None) -> List[OfferRecord]:
    now = time.time()
    out = [
        o for o in store.state.offers.values()
        if not o.filled and o.amount > 0 and o.expires_at > now
    ]
    if collectible_id:
        out = [o for o in out if o.collectible_id == collectible_id]
    return sorted(out, key=lambda x: x.created_at)


def follower_count(store: UniteStore, creator_id: str) -> int:
    return sum(1 for f in store.state.fan_follows if f.creator_id == creator_id)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def cmd_register_creator(app: UniteApp, args: argparse.Namespace) -> None:
    content_root = args.content_root or content_hash_str(str(random.random()))
    rec = app.register_creator(
        account=args.account,
        content_root=content_root,
        handle=args.handle,
    )
    print(f"Registered creator: {rec.creator_id} ({rec.handle})")
    app._store.save()


def cmd_mint(app: UniteApp, args: argparse.Namespace) -> None:
    content_hash = args.content_hash or content_hash_str(str(time.time()))
    rec = app.mint_collectible(
        creator_id=args.creator_id,
        account=args.account,
        content_hash=content_hash,
        supply_cap=args.supply_cap,
        to=args.to,
    )
    print(f"Minted collectible: {rec.collectible_id} to {args.to}")
    app._store.save()


def cmd_follow(app: UniteApp, args: argparse.Namespace) -> None:
    app.follow(creator_id=args.creator_id, fan=args.fan)
    print(f"Followed creator {args.creator_id}")
    app._store.save()


def cmd_list_creators(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    creators = list_creators(store, offset=args.offset, limit=args.limit)
    for c in creators:
        cnt = follower_count(store, c.creator_id)
        print(f"{c.creator_id} | {c.handle} | {c.account} | followers={cnt} | active={c.active}")
    print(f"Total shown: {len(creators)}")


def cmd_list_collectibles(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    if args.creator_id:
        colls = list_collectibles_by_creator(store, args.creator_id)
    else:
        colls = list_collectibles(store, offset=args.offset, limit=args.limit)
    for c in colls:
        print(f"{c.collectible_id} | creator={c.creator_id} | supply={c.total_minted}/{c.supply_cap} | frozen={c.frozen}")
    print(f"Total shown: {len(colls)}")


def cmd_list_listings(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    listings = list_active_listings(store, collectible_id=args.collectible_id or None)
    for l in listings:
        print(f"{l.listing_id} | col={l.collectible_id} | seller={l.seller} | amount={l.amount} | price_wei={l.price_wei}")
    print(f"Total shown: {len(listings)}")


def cmd_list_offers(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    offers = list_active_offers(store, collectible_id=args.collectible_id or None)
    for o in offers:
        print(f"{o.offer_id} | col={o.collectible_id} | bidder={o.bidder} | amount={o.amount} | price_wei={o.price_wei}")
    print(f"Total shown: {len(offers)}")


def cmd_balance(app: UniteApp, args: argparse.Namespace) -> None:
    bal = app._store.balance_of(args.collectible_id, args.account)
    print(f"Balance: {bal}")


def cmd_create_listing(app: UniteApp, args: argparse.Namespace) -> None:
    duration = args.duration or 604800
    rec = app.create_listing(
        collectible_id=args.collectible_id,
        seller=args.seller,
        amount=args.amount,
        price_wei=args.price_wei,
        duration_seconds=duration,
    )
    print(f"Created listing: {rec.listing_id}")
    app._store.save()


def cmd_place_offer(app: UniteApp, args: argparse.Namespace) -> None:
    duration = args.duration or 259200
    rec = app.place_offer(
        collectible_id=args.collectible_id,
        bidder=args.bidder,
        amount=args.amount,
        price_wei=args.price_wei,
        duration_seconds=duration,
    )
    print(f"Placed offer: {rec.offer_id}")
    app._store.save()


def cmd_stats(app: UniteApp, args: argparse.Namespace) -> None:
    s = app._store.state
    print(f"Creators: {len(s.creators)}")
    print(f"Collectibles: {len(s.collectibles)}")
    print(f"Listings: {len(s.listings)}")
    print(f"Offers: {len(s.offers)}")
    print(f"Fan follows: {len(s.fan_follows)}")
    print(f"Next creator num: {s.next_creator_num}")
    print(f"Next collectible num: {s.next_collectible_num}")
    print(f"Next listing num: {s.next_listing_num}")
    print(f"Next offer num: {s.next_offer_num}")


# -----------------------------------------------------------------------------
# REST API HANDLER
# -----------------------------------------------------------------------------


class UniteAPIHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/api/creators":
            self._get_creators()
        elif path == "/api/collectibles":
            self._get_collectibles()
        elif path.startswith("/api/creator/"):
            self._get_creator(path.replace("/api/creator/", ""))
        elif path.startswith("/api/collectible/"):
            self._get_collectible(path.replace("/api/collectible/", ""))
        elif path == "/api/listings":
            self._get_listings()
        elif path == "/api/offers":
            self._get_offers()
        elif path == "/api/stats":
            self._get_stats()
        elif path == "/" or path == "/index.html":
            self._serve_index()
        else:
            self.send_error(404)

    def _get_creators(self) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        offset = int(self._query().get("offset", 0))
        limit = int(self._query().get("limit", 50))
        creators = list_creators(store, offset=offset, limit=limit)
        data = [
            {
                "creator_id": c.creator_id,
                "handle": c.handle,
                "account": c.account,
                "active": c.active,
                "registered_at": c.registered_at,
                "follower_count": follower_count(store, c.creator_id),
            }
            for c in creators
        ]
        self._json_response({"creators": data, "count": len(data)})

    def _get_collectibles(self) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        creator_id = self._query().get("creator_id")
        offset = int(self._query().get("offset", 0))
        limit = int(self._query().get("limit", 50))
        if creator_id:
            colls = list_collectibles_by_creator(store, creator_id)
        else:
            colls = list_collectibles(store, offset=offset, limit=limit)
        data = [
            {
                "collectible_id": c.collectible_id,
                "creator_id": c.creator_id,
                "content_hash": c.content_hash,
                "supply_cap": c.supply_cap,
                "total_minted": c.total_minted,
                "frozen": c.frozen,
            }
            for c in colls
        ]
        self._json_response({"collectibles": data, "count": len(data)})

    def _get_creator(self, creator_id: str) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        try:
            c = store.get_creator(creator_id)
            self._json_response({
                "creator_id": c.creator_id,
                "handle": c.handle,
                "account": c.account,
                "content_root": c.content_root,
                "active": c.active,
                "registered_at": c.registered_at,
                "updated_at": c.updated_at,
                "follower_count": follower_count(store, creator_id),
            })
        except UniteNotFoundError:
            self._json_response({"error": "Creator not found"}, 404)

    def _get_collectible(self, collectible_id: str) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        try:
            c = store.get_collectible(collectible_id)
            royalty = store.state.royalty_configs.get(collectible_id)
            self._json_response({
                "collectible_id": c.collectible_id,
                "creator_id": c.creator_id,
                "content_hash": c.content_hash,
                "supply_cap": c.supply_cap,
                "total_minted": c.total_minted,
                "frozen": c.frozen,
                "minted_at": c.minted_at,
                "royalty_recipient": royalty.recipient if royalty else None,
                "royalty_bps": royalty.bps if royalty else None,
            })
        except UniteNotFoundError:
            self._json_response({"error": "Collectible not found"}, 404)

    def _get_listings(self) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        collectible_id = self._query().get("collectible_id")
        listings = list_active_listings(store, collectible_id=collectible_id or None)
        data = [
            {
                "listing_id": l.listing_id,
                "collectible_id": l.collectible_id,
                "seller": l.seller,
                "amount": l.amount,
                "price_wei": l.price_wei,
                "expires_at": l.expires_at,
            }
            for l in listings
        ]
        self._json_response({"listings": data, "count": len(data)})

    def _get_offers(self) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        collectible_id = self._query().get("collectible_id")
        offers = list_active_offers(store, collectible_id=collectible_id or None)
        data = [
            {
                "offer_id": o.offer_id,
                "collectible_id": o.collectible_id,
                "bidder": o.bidder,
                "amount": o.amount,
                "price_wei": o.price_wei,
                "expires_at": o.expires_at,
            }
            for o in offers
        ]
        self._json_response({"offers": data, "count": len(data)})

    def _get_stats(self) -> None:
        store = getattr(self.server, "unite_store", None)
        if not store:
            self._json_response({"error": "Store not configured"}, 500)
            return
        s = store.state
        self._json_response({
            "creators": len(s.creators),
            "collectibles": len(s.collectibles),
            "listings": len(s.listings),
            "offers": len(s.offers),
            "fan_follows": len(s.fan_follows),
        })

    def _serve_index(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"<html><body><h1>Unite API</h1><p>Endpoints: /api/creators, /api/collectibles, /api/listings, /api/offers, /api/stats</p></body></html>")

    def _query(self) -> Dict[str, str]:
        q = self.path.split("?")[-1] if "?" in self.path else ""
        out: Dict[str, str] = {}
        for part in q.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k] = v
        return out

    def _json_response(self, data: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        if os.environ.get("UNITE_DEBUG"):
            super().log_message(format, *args)


# -----------------------------------------------------------------------------
# RPC CLIENT STUB (optional web3)
# -----------------------------------------------------------------------------

try:
    from web3 import Web3
    _HAS_WEB3 = True
except ImportError:
    _HAS_WEB3 = False


class SiamsoRpcClient:
    """Optional RPC client for SiamsoProtocol contract. Use when web3 is installed."""

    def __init__(
        self,
        rpc_url: str = UNITE_DEFAULT_RPC,
        contract_address: Optional[str] = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.contract_address = contract_address
        self._w3: Any = None
        self._contract: Any = None

    def connect(self) -> bool:
        if not _HAS_WEB3:
            return False
        try:
            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url))
            return self._w3.is_connected()
        except Exception:
            return False

    def get_total_creators(self) -> Optional[int]:
        if not _HAS_WEB3 or not self._contract:
            return None
        try:
            return self._contract.functions.totalCreators().call()
        except Exception:
            return None

    def get_total_collectibles(self) -> Optional[int]:
        if not _HAS_WEB3 or not self._contract:
            return None
        try:
            return self._contract.functions.totalCollectibles().call()
        except Exception:
            return None


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(prog=UNITE_APP_NAME, description="Unite - SiamsoProtocol companion app")
    parser.add_argument("--state", default=UNITE_STATE_FILE, help="State file path")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # register-creator
    p = subparsers.add_parser("register-creator")
    p.add_argument("--account", required=True)
    p.add_argument("--handle", required=True)
    p.add_argument("--content-root", default=None)
    p.set_defaults(func=cmd_register_creator)

    # mint
    p = subparsers.add_parser("mint")
    p.add_argument("--creator-id", required=True)
    p.add_argument("--account", required=True)
    p.add_argument("--to", required=True)
    p.add_argument("--supply-cap", type=int, default=1)
    p.add_argument("--content-hash", default=None)
    p.set_defaults(func=cmd_mint)

    # follow
    p = subparsers.add_parser("follow")
    p.add_argument("--creator-id", required=True)
    p.add_argument("--fan", required=True)
    p.set_defaults(func=cmd_follow)

    # list creators
    p = subparsers.add_parser("list-creators")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_list_creators)

    # list collectibles
    p = subparsers.add_parser("list-collectibles")
    p.add_argument("--creator-id", default=None)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_list_collectibles)

    # list listings
    p = subparsers.add_parser("list-listings")
    p.add_argument("--collectible-id", default=None)
    p.set_defaults(func=cmd_list_listings)

    # list offers
    p = subparsers.add_parser("list-offers")
    p.add_argument("--collectible-id", default=None)
    p.set_defaults(func=cmd_list_offers)

    # balance
    p = subparsers.add_parser("balance")
    p.add_argument("--collectible-id", required=True)
    p.add_argument("--account", required=True)
    p.set_defaults(func=cmd_balance)

    # create-listing
    p = subparsers.add_parser("create-listing")
    p.add_argument("--collectible-id", required=True)
    p.add_argument("--seller", required=True)
    p.add_argument("--amount", type=int, required=True)
    p.add_argument("--price-wei", type=int, required=True)
    p.add_argument("--duration", type=int, default=None)
    p.set_defaults(func=cmd_create_listing)

    # place-offer
    p = subparsers.add_parser("place-offer")
    p.add_argument("--collectible-id", required=True)
    p.add_argument("--bidder", required=True)
    p.add_argument("--amount", type=int, required=True)
    p.add_argument("--price-wei", type=int, required=True)
    p.add_argument("--duration", type=int, default=None)
    p.set_defaults(func=cmd_place_offer)

    # stats
    p = subparsers.add_parser("stats")
    p.set_defaults(func=cmd_stats)

    # serve
    p = subparsers.add_parser("serve")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.set_defaults(func=lambda app, args: run_server(app, args))

    _add_extra_commands(subparsers)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0

    store = UniteStore(state_path=Path(args.state))
    try:
        store.load()
    except UniteStateError:
        pass
    app = UniteApp(store)

    try:
        args.func(app, args)
    except (UniteError, KeyError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def run_server(app: UniteApp, args: argparse.Namespace) -> None:
    host = args.host
    port = args.port
    server = HTTPServer((host, port), UniteAPIHandler)
    server.unite_store = app._store
    print(f"Serving Unite API at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


# -----------------------------------------------------------------------------
# EXPORT / IMPORT
# -----------------------------------------------------------------------------


def export_state_json(store: UniteStore) -> str:
    data = store._serialize()
    return json.dumps(data, indent=2)


def import_state_json(store: UniteStore, json_str: str) -> None:
    data = json.loads(json_str)
    store._deserialize(data)


def cmd_export(app: UniteApp, args: argparse.Namespace) -> None:
    path = getattr(args, "output", None) or "unite_export.json"
    s = export_state_json(app._store)
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)
    print(f"Exported to {path}")


def cmd_import(app: UniteApp, args: argparse.Namespace) -> None:
    path = getattr(args, "input", None) or "unite_export.json"
    with open(path, "r", encoding="utf-8") as f:
        import_state_json(app._store, f.read())
    app._store.save()
    print(f"Imported from {path}")


# -----------------------------------------------------------------------------
# CONFIG LOADER
# -----------------------------------------------------------------------------


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    path = config_path or Path("unite_config.json")
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_config(config: Dict[str, Any], config_path: Optional[Path] = None) -> None:
    path = config_path or Path("unite_config.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


# -----------------------------------------------------------------------------
# ANALYTICS HELPERS
# -----------------------------------------------------------------------------


def creator_stats(store: UniteStore, creator_id: str) -> Dict[str, Any]:
    try:
        c = store.get_creator(creator_id)
    except UniteNotFoundError:
        return {}
    colls = list_collectibles_by_creator(store, creator_id)
    followers = follower_count(store, creator_id)
    return {
        "creator_id": c.creator_id,
        "handle": c.handle,
        "account": c.account,
        "active": c.active,
        "collectible_count": len(colls),
        "follower_count": followers,
        "total_supply_minted": sum(x.total_minted for x in colls),
    }


def collectible_stats(store: UniteStore, collectible_id: str) -> Dict[str, Any]:
    try:
        c = store.get_collectible(collectible_id)
    except UniteNotFoundError:
        return {}
    listings = list_active_listings(store, collectible_id=collectible_id)
    offers = list_active_offers(store, collectible_id=collectible_id)
    total_held = sum(
        store.balance_of(collectible_id, a)
        for (col, a) in store.state.collectible_balances
        if col == collectible_id
    )
    return {
        "collectible_id": c.collectible_id,
        "creator_id": c.creator_id,
        "supply_cap": c.supply_cap,
        "total_minted": c.total_minted,
        "active_listings": len(listings),
        "active_offers": len(offers),
        "total_held": total_held,
    }


def protocol_stats(store: UniteStore) -> Dict[str, Any]:
    s = store.state
    return {
        "creators": len(s.creators),
        "collectibles": len(s.collectibles),
        "listings": len(s.listings),
        "offers": len(s.offers),
        "fan_follows": len(s.fan_follows),
        "next_creator_num": s.next_creator_num,
        "next_collectible_num": s.next_collectible_num,
        "next_listing_num": s.next_listing_num,
        "next_offer_num": s.next_offer_num,
    }


# -----------------------------------------------------------------------------
# POST HANDLERS FOR REST API
# -----------------------------------------------------------------------------


def parse_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", 0))
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    return json.loads(raw.decode("utf-8"))


def UniteAPIHandler_do_POST(handler: UniteAPIHandler) -> None:
    path = handler.path.split("?")[0]
    store = getattr(handler.server, "unite_store", None)
    if not store:
        handler._json_response({"error": "Store not configured"}, 500)
        return
    app = UniteApp(store)
    try:
        body = parse_body(handler)
    except (json.JSONDecodeError, ValueError):
        handler._json_response({"error": "Invalid JSON body"}, 400)
        return
    if path == "/api/register-creator":
        try:
            rec = app.register_creator(
                account=body["account"],
                content_root=body.get("content_root") or content_hash_str(str(random.random())),
                handle=body["handle"],
            )
            store.save()
            handler._json_response({"creator_id": rec.creator_id, "handle": rec.handle}, 201)
        except (KeyError, UniteError) as e:
            handler._json_response({"error": str(e)}, 400)
    elif path == "/api/follow":
        try:
            app.follow(creator_id=body["creator_id"], fan=body["fan"])
            store.save()
            handler._json_response({"ok": True}, 201)
        except (KeyError, UniteError) as e:
            handler._json_response({"error": str(e)}, 400)
    elif path == "/api/mint":
        try:
            rec = app.mint_collectible(
                creator_id=body["creator_id"],
                account=body["account"],
                content_hash=body.get("content_hash") or content_hash_str(str(time.time())),
                supply_cap=int(body.get("supply_cap", 1)),
                to=body["to"],
            )
            store.save()
            handler._json_response({"collectible_id": rec.collectible_id}, 201)
        except (KeyError, UniteError) as e:
            handler._json_response({"error": str(e)}, 400)
    else:
        handler.send_error(404)


# Monkey-patch POST
_original_UniteAPIHandler_do_GET = UniteAPIHandler.do_GET
def _UniteAPIHandler_do_POST(self: UniteAPIHandler) -> None:
    UniteAPIHandler_do_POST(self)
UniteAPIHandler.do_POST = _UniteAPIHandler_do_POST


# -----------------------------------------------------------------------------
# ADDITIONAL CLI COMMANDS
# -----------------------------------------------------------------------------


def cmd_creator_stats(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    stats = creator_stats(store, args.creator_id)
    if not stats:
        print("Creator not found")
        return
    for k, v in stats.items():
        print(f"{k}: {v}")


def cmd_collectible_stats(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    stats = collectible_stats(store, args.collectible_id)
    if not stats:
        print("Collectible not found")
        return
    for k, v in stats.items():
        print(f"{k}: {v}")


def cmd_protocol_stats(app: UniteApp, args: argparse.Namespace) -> None:
    store = app._store
    for k, v in protocol_stats(store).items():
        print(f"{k}: {v}")


def cmd_content_hash(app: UniteApp, args: argparse.Namespace) -> None:
    payload = getattr(args, "payload", " ") or " "
    h = content_hash_str(payload)
    print(f"content_hash: {h}")


def cmd_unfollow(app: UniteApp, args: argparse.Namespace) -> None:
    app.unfollow(creator_id=args.creator_id, fan=args.fan)
    print(f"Unfollowed creator {args.creator_id}")
    app._store.save()


def cmd_set_royalty(app: UniteApp, args: argparse.Namespace) -> None:
    app.set_royalty(
        collectible_id=args.collectible_id,
        creator_account=args.account,
        recipient=args.recipient,
        bps=args.bps,
    )
    print("Royalty set")
    app._store.save()


def cmd_cancel_listing(app: UniteApp, args: argparse.Namespace) -> None:
    app.cancel_listing(listing_id=args.listing_id, seller=args.seller)
    print("Listing cancelled")
    app._store.save()


def cmd_cancel_offer(app: UniteApp, args: argparse.Namespace) -> None:
    app.cancel_offer(offer_id=args.offer_id, bidder=args.bidder)
    print("Offer cancelled")
    app._store.save()


# -----------------------------------------------------------------------------
# RPC CLIENT EXTENDED
# -----------------------------------------------------------------------------


def siamso_contract_abi_minimal() -> List[Dict[str, Any]]:
    """Minimal ABI for read-only calls to SiamsoProtocol."""
    return [
        {"inputs": [], "name": "totalCreators", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
        {"inputs": [], "name": "totalCollectibles", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
