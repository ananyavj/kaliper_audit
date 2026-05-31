"""
run_tests.py
Comprehensive test harness for kaliper_audit.
Runs many scenarios across all three plan types and prints a full report.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from uuid import uuid4
from datetime import datetime, timezone
from core.schemas import IncomingEvent, TrackingEventSpec
from core.plan_loader import load_tracking_plan
from core.plan_normalizer import normalize_specs
from core.plan_analyzer import analyze_tracking_plan
from core.detectors import detect_issues
from core.state_store import StreamState, make_state_from_profile


def _now():
    return datetime.now(timezone.utc).isoformat()


def ev(name, props, user_id="u1", anon_id="anon1"):
    return IncomingEvent(
        name=name, timestamp=_now(), properties=props,
        event_id=str(uuid4()), user_id=user_id, anonymous_id=anon_id,
    )


# ─────────────────────────────────────────────
# ECOMMERCE SCENARIOS
# ─────────────────────────────────────────────

def ecom_happy_path():
    "Full clean funnel: page -> product -> add -> checkout -> purchase"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/shoes", "session_id": "s1"}),
        ev("Product Viewed",   {"product_id": "p1", "title": "Nike Air", "price": 4999.0}),
        ev("Product Added",    {"product_id": "p1", "quantity": 1, "price": 4999.0}),
        ev("Checkout Started", {"cart_id": "c1", "revenue": 4999.0, "currency": "INR"}),
        ev("Checkout Step Viewed", {"cart_id": "c1", "step": 1}),
        ev("Checkout Step Viewed", {"cart_id": "c1", "step": 2}),
        ev("Order Completed",  {"order_id": "ord_1", "revenue": 4999.0, "currency": "INR"}),
    ]


def ecom_refund_after_purchase():
    "Valid refund following a real order"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s2"}),
        ev("Product Viewed",   {"product_id": "p2", "title": "Adidas Stan Smith", "price": 3499.0}),
        ev("Product Added",    {"product_id": "p2", "quantity": 1, "price": 3499.0}),
        ev("Checkout Started", {"cart_id": "c2", "revenue": 3499.0, "currency": "INR"}),
        ev("Order Completed",  {"order_id": "ord_2", "revenue": 3499.0, "currency": "INR"}),
        ev("Order Refunded",   {"order_id": "ord_2", "refund_amount": 3499.0}),
    ]


def ecom_coupon_valid():
    "Coupon applied correctly inside an active checkout"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s3"}),
        ev("Product Viewed",   {"product_id": "p3", "title": "Puma Suede", "price": 2999.0}),
        ev("Product Added",    {"product_id": "p3", "quantity": 2, "price": 2999.0}),
        ev("Checkout Started", {"cart_id": "c3", "revenue": 5998.0, "currency": "INR"}),
        ev("Coupon Applied",   {"coupon_code": "SAVE10", "discount_amount": 599.8}),
        ev("Order Completed",  {"order_id": "ord_3", "revenue": 5398.2, "currency": "INR"}),
    ]


def ecom_wishlist_no_login():
    "Wishlist add without user_id -- should raise wishlist_without_identity"
    return [
        ev("Page Viewed",    {"page_url": "https://shop.com/", "session_id": "s4"},
           user_id=None, anon_id="anon4"),
        ev("Product Viewed", {"product_id": "p4", "title": "Reebok Classic", "price": 3999.0},
           user_id=None, anon_id="anon4"),
        ev("Wishlist Added", {"product_id": "p4"},
           user_id=None, anon_id="anon4"),
    ]


def ecom_purchase_without_checkout():
    "Order Completed with no prior Checkout Started -- purchase_without_checkout"
    return [
        ev("Page Viewed",    {"page_url": "https://shop.com/", "session_id": "s5"}),
        ev("Product Viewed", {"product_id": "p5", "title": "Converse", "price": 2499.0}),
        ev("Product Added",  {"product_id": "p5", "quantity": 1, "price": 2499.0}),
        ev("Order Completed", {"order_id": "ord_5", "revenue": 2499.0, "currency": "INR"}),
    ]


def ecom_duplicate_purchase():
    "Same order_id submitted twice -- duplicate_purchase"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s6"}),
        ev("Product Viewed",   {"product_id": "p6", "title": "Vans Old Skool", "price": 3299.0}),
        ev("Product Added",    {"product_id": "p6", "quantity": 1, "price": 3299.0}),
        ev("Checkout Started", {"cart_id": "c6", "revenue": 3299.0, "currency": "INR"}),
        ev("Order Completed",  {"order_id": "ord_6", "revenue": 3299.0, "currency": "INR"}),
        ev("Order Completed",  {"order_id": "ord_6", "revenue": 3299.0, "currency": "INR"}),
    ]


def ecom_bad_currency():
    "Currency sent as full word 'rupees' instead of ISO code -- invalid_currency"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s7"}),
        ev("Product Viewed",   {"product_id": "p7", "title": "Crocs", "price": 1999.0}),
        ev("Product Added",    {"product_id": "p7", "quantity": 1, "price": 1999.0}),
        ev("Checkout Started", {"cart_id": "c7", "revenue": 1999.0, "currency": "rupees"}),
        ev("Order Completed",  {"order_id": "ord_7", "revenue": 1999.0, "currency": "rupees"}),
    ]


def ecom_revenue_as_string():
    "Revenue sent as a string '999.0' -- wrong_property_type"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s8"}),
        ev("Product Viewed",   {"product_id": "p8", "title": "Birkenstock", "price": 5999.0}),
        ev("Product Added",    {"product_id": "p8", "quantity": 1, "price": 5999.0}),
        ev("Checkout Started", {"cart_id": "c8", "revenue": "5999.0", "currency": "INR"}),
        ev("Order Completed",  {"order_id": "ord_8", "revenue": "5999.0", "currency": "INR"}),
    ]


def ecom_missing_required_prop():
    "Order Completed missing 'currency' -- missing_required_property"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "s9"}),
        ev("Product Viewed",   {"product_id": "p9", "title": "Timberland", "price": 8999.0}),
        ev("Product Added",    {"product_id": "p9", "quantity": 1, "price": 8999.0}),
        ev("Checkout Started", {"cart_id": "c9", "revenue": 8999.0, "currency": "USD"}),
        ev("Order Completed",  {"order_id": "ord_9", "revenue": 8999.0}),
    ]


def ecom_refund_without_purchase():
    "Refund for an order_id never purchased this session -- return_without_purchase"
    return [
        ev("Page Viewed",    {"page_url": "https://shop.com/", "session_id": "s10"}),
        ev("Order Refunded", {"order_id": "ord_ghost", "refund_amount": 1000.0}),
    ]


def ecom_checkout_step_regression():
    "Step goes 1 -> 2 -> 1 -- checkout_step_regression"
    return [
        ev("Page Viewed",          {"page_url": "https://shop.com/", "session_id": "s11"}),
        ev("Product Viewed",       {"product_id": "p11", "title": "Shoe", "price": 999.0}),
        ev("Product Added",        {"product_id": "p11", "quantity": 1, "price": 999.0}),
        ev("Checkout Started",     {"cart_id": "c11", "revenue": 999.0, "currency": "INR"}),
        ev("Checkout Step Viewed", {"cart_id": "c11", "step": 1}),
        ev("Checkout Step Viewed", {"cart_id": "c11", "step": 2}),
        ev("Checkout Step Viewed", {"cart_id": "c11", "step": 1}),
    ]


def ecom_coupon_without_checkout():
    "Coupon Applied before Checkout Started -- coupon_without_checkout"
    return [
        ev("Page Viewed",    {"page_url": "https://shop.com/", "session_id": "s12"}),
        ev("Product Viewed", {"product_id": "p12", "title": "Shoe", "price": 999.0}),
        ev("Coupon Applied", {"coupon_code": "EARLY10", "discount_amount": 99.9}),
    ]


def ecom_unknown_event():
    "Fires an event not in the plan -- unknown_event"
    return [
        ev("Page Viewed",    {"page_url": "https://shop.com/", "session_id": "s13"}),
        ev("Cart Merged",    {"cart_id": "c13", "merged_from": "guest_cart"}),
        ev("Product Viewed", {"product_id": "p13", "title": "Boot", "price": 2499.0}),
    ]


def ecom_duplicate_event_id():
    "Same event_id sent twice -- duplicate_event_id"
    fixed_id = str(uuid4())
    return [
        IncomingEvent("Page Viewed", _now(), {"page_url": "https://shop.com/", "session_id": "s14"},
                      event_id=fixed_id, user_id="u14", anonymous_id="anon14"),
        IncomingEvent("Page Viewed", _now(), {"page_url": "https://shop.com/", "session_id": "s14"},
                      event_id=fixed_id, user_id="u14", anonymous_id="anon14"),
    ]


def ecom_multi_user_session():
    "Two users completing valid purchases in the same batch -- no issues expected"
    return [
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "sa"}, user_id="uA", anon_id="anonA"),
        ev("Product Viewed",   {"product_id": "pA", "title": "Item A", "price": 1000.0}, user_id="uA", anon_id="anonA"),
        ev("Product Added",    {"product_id": "pA", "quantity": 1, "price": 1000.0}, user_id="uA", anon_id="anonA"),
        ev("Checkout Started", {"cart_id": "cA", "revenue": 1000.0, "currency": "USD"}, user_id="uA", anon_id="anonA"),
        ev("Order Completed",  {"order_id": "ordA", "revenue": 1000.0, "currency": "USD"}, user_id="uA", anon_id="anonA"),
        ev("Page Viewed",      {"page_url": "https://shop.com/", "session_id": "sb"}, user_id="uB", anon_id="anonB"),
        ev("Product Viewed",   {"product_id": "pB", "title": "Item B", "price": 2000.0}, user_id="uB", anon_id="anonB"),
        ev("Product Added",    {"product_id": "pB", "quantity": 2, "price": 2000.0}, user_id="uB", anon_id="anonB"),
        ev("Checkout Started", {"cart_id": "cB", "revenue": 4000.0, "currency": "AED"}, user_id="uB", anon_id="anonB"),
        ev("Order Completed",  {"order_id": "ordB", "revenue": 4000.0, "currency": "AED"}, user_id="uB", anon_id="anonB"),
    ]


# ─────────────────────────────────────────────
# SAAS SCENARIOS
# ─────────────────────────────────────────────

def saas_happy_path():
    "Full clean SaaS funnel: signup -> login -> onboarding -> trial -> subscription"
    return [
        ev("Signup",               {"account_id": "acc1", "plan": "free", "signup_method": "google"}),
        ev("Login",                {"method": "google"}),
        ev("Onboarding Completed", {"account_id": "acc1", "steps_completed": 5}),
        ev("Trial Started",        {"trial_id": "tr1", "plan": "pro", "trial_length_days": 14}),
        ev("Feature Used",         {"feature_name": "analytics_dashboard", "account_id": "acc1"}),
        ev("Invite Sent",          {"invitee_email": "colleague@co.com", "account_id": "acc1"}),
        ev("Subscription Started", {"subscription_id": "sub1", "plan": "pro", "billing_status": "active", "mrr": 2999.0}),
    ]


def saas_login_before_signup():
    "Login fires before Signup in session -- sequence_error"
    return [
        ev("Login",   {"method": "password"}),
        ev("Signup",  {"account_id": "acc2", "plan": "free", "signup_method": "password"}),
    ]


def saas_trial_missing_identity():
    "Trial Started with no user_id -- missing_identity"
    return [
        ev("Signup",        {"account_id": "acc3", "plan": "free", "signup_method": "github"}, user_id="u3"),
        ev("Trial Started", {"trial_id": "tr3", "plan": "pro", "trial_length_days": 7},
           user_id=None, anon_id="anon3"),
    ]


def saas_invalid_plan_enum():
    "Plan value 'basic_plus' not in allowed list -- enum_value_violation"
    return [
        ev("Signup",        {"account_id": "acc4", "plan": "free", "signup_method": "password"}),
        ev("Trial Started", {"trial_id": "tr4", "plan": "basic_plus", "trial_length_days": 14}),
    ]


def saas_invalid_login_method():
    "Login method 'magic_link' not in allowed list -- enum_value_violation"
    return [
        ev("Signup", {"account_id": "acc5", "plan": "free", "signup_method": "password"}),
        ev("Login",  {"method": "magic_link"}),
    ]


def saas_subscription_missing_mrr():
    "Subscription Started without mrr -- missing_required_property"
    return [
        ev("Signup",               {"account_id": "acc6", "plan": "free", "signup_method": "google"}),
        ev("Trial Started",        {"trial_id": "tr6", "plan": "starter", "trial_length_days": 14}),
        ev("Subscription Started", {"subscription_id": "sub6", "plan": "starter", "billing_status": "active"}),
    ]


def saas_subscription_without_trial():
    "Subscription Started without a prior Trial -- sequence_error"
    return [
        ev("Signup",               {"account_id": "acc7", "plan": "free", "signup_method": "sso"}),
        ev("Subscription Started", {"subscription_id": "sub7", "plan": "pro", "billing_status": "active", "mrr": 4999.0}),
    ]


def saas_cancel_after_subscription():
    "Clean cancellation after active subscription"
    return [
        ev("Signup",                 {"account_id": "acc8", "plan": "free", "signup_method": "google"}),
        ev("Trial Started",          {"trial_id": "tr8", "plan": "pro", "trial_length_days": 14}),
        ev("Subscription Started",   {"subscription_id": "sub8", "plan": "pro", "billing_status": "active", "mrr": 2999.0}),
        ev("Subscription Cancelled", {"subscription_id": "sub8", "reason": "too_expensive"}),
    ]


def saas_mrr_as_boolean():
    "MRR sent as boolean True -- wrong_property_type"
    return [
        ev("Signup",               {"account_id": "acc9", "plan": "free", "signup_method": "github"}),
        ev("Trial Started",        {"trial_id": "tr9", "plan": "starter", "trial_length_days": 14}),
        ev("Subscription Started", {"subscription_id": "sub9", "plan": "starter", "billing_status": "active", "mrr": True}),
    ]


def saas_unknown_event():
    "Fires 'Password Changed' which is not in the plan -- unknown_event"
    return [
        ev("Signup",           {"account_id": "acc10", "plan": "free", "signup_method": "password"}),
        ev("Login",            {"method": "password"}),
        ev("Password Changed", {"account_id": "acc10"}),
    ]


def saas_multi_user():
    "Two users on separate plans, both clean"
    return [
        ev("Signup",               {"account_id": "accX", "plan": "free", "signup_method": "google"}, user_id="uX"),
        ev("Trial Started",        {"trial_id": "trX", "plan": "pro", "trial_length_days": 14}, user_id="uX"),
        ev("Subscription Started", {"subscription_id": "subX", "plan": "pro", "billing_status": "active", "mrr": 2999.0}, user_id="uX"),
        ev("Signup",               {"account_id": "accY", "plan": "free", "signup_method": "github"}, user_id="uY"),
        ev("Trial Started",        {"trial_id": "trY", "plan": "enterprise", "trial_length_days": 30}, user_id="uY"),
        ev("Subscription Started", {"subscription_id": "subY", "plan": "enterprise", "billing_status": "active", "mrr": 14999.0}, user_id="uY"),
    ]


# ─────────────────────────────────────────────
# CONTENT SCENARIOS
# ─────────────────────────────────────────────

def content_happy_path():
    "Full clean content session: page -> article -> share -> video -> complete"
    return [
        ev("Page Viewed",        {"page_url": "https://media.com/tech", "session_id": "cs1"}),
        ev("Article Read",       {"article_id": "a1", "title": "AI in 2025", "duration_seconds": 180, "scroll_depth_pct": 95}),
        ev("Article Shared",     {"article_id": "a1", "channel": "twitter"}),
        ev("Video Started",      {"video_id": "v1", "title": "Demo Reel", "duration_seconds": 600}),
        ev("Video Paused",       {"video_id": "v1", "position_seconds": 120}),
        ev("Video Completed",    {"video_id": "v1", "duration_seconds": 600, "watch_pct": 100}),
        ev("Content Bookmarked", {"content_id": "v1", "content_type": "video"}),
    ]


def content_article_before_page_view():
    "Article Read before Page Viewed -- sequence_error"
    return [
        ev("Article Read", {"article_id": "a2", "title": "No Page", "duration_seconds": 90, "scroll_depth_pct": 50}),
        ev("Page Viewed",  {"page_url": "https://media.com/health", "session_id": "cs2"}),
    ]


def content_video_complete_without_start():
    "Video Completed without Video Started -- sequence_error"
    return [
        ev("Page Viewed",     {"page_url": "https://media.com/sports", "session_id": "cs3"}),
        ev("Video Completed", {"video_id": "v3", "duration_seconds": 300, "watch_pct": 100}),
    ]


def content_bad_share_channel():
    "Article Shared with channel 'telegram' not in allowed values -- enum_value_violation"
    return [
        ev("Page Viewed",    {"page_url": "https://media.com/", "session_id": "cs4"}),
        ev("Article Read",   {"article_id": "a4", "title": "Sports", "duration_seconds": 120, "scroll_depth_pct": 60}),
        ev("Article Shared", {"article_id": "a4", "channel": "telegram"}),
    ]


def content_duration_as_string():
    "duration_seconds sent as string -- wrong_property_type"
    return [
        ev("Page Viewed",  {"page_url": "https://media.com/", "session_id": "cs5"}),
        ev("Article Read", {"article_id": "a5", "title": "Finance", "duration_seconds": "200", "scroll_depth_pct": 80}),
    ]


def content_missing_scroll_depth():
    "Article Read missing scroll_depth_pct -- missing_required_property"
    return [
        ev("Page Viewed",  {"page_url": "https://media.com/", "session_id": "cs6"}),
        ev("Article Read", {"article_id": "a6", "title": "Health", "duration_seconds": 150}),
    ]


def content_bad_bookmark_type():
    "Content Bookmarked with content_type 'reel' not in allowed values -- enum_value_violation"
    return [
        ev("Page Viewed",        {"page_url": "https://media.com/", "session_id": "cs7"}),
        ev("Content Bookmarked", {"content_id": "c7", "content_type": "reel"}),
    ]


def content_subscription():
    "User subscribes after viewing a page -- valid"
    return [
        ev("Page Viewed",          {"page_url": "https://media.com/subscribe", "session_id": "cs8"}),
        ev("Subscription Started", {"plan": "premium", "billing_cycle": "annual"}),
    ]


def content_invalid_subscription_plan():
    "Subscription plan 'gold' not in allowed values -- enum_value_violation"
    return [
        ev("Page Viewed",          {"page_url": "https://media.com/subscribe", "session_id": "cs9"}),
        ev("Subscription Started", {"plan": "gold", "billing_cycle": "monthly"}),
    ]


def content_search_no_identity():
    "Search is anonymous -- should be clean"
    return [
        ev("Search Performed", {"query": "python tutorials", "result_count": 42},
           user_id=None, anon_id="anonS"),
        ev("Search Performed", {"query": "machine learning", "result_count": 18},
           user_id=None, anon_id="anonS"),
    ]


def content_watch_pct_missing():
    "Video Completed missing watch_pct -- missing_required_property"
    return [
        ev("Page Viewed",     {"page_url": "https://media.com/", "session_id": "cs11"}),
        ev("Video Started",   {"video_id": "v11", "title": "Tutorial", "duration_seconds": 900}),
        ev("Video Completed", {"video_id": "v11", "duration_seconds": 900}),
    ]


def content_multi_user():
    "Three readers browsing independently -- all clean"
    events = []
    for i in range(3):
        uid = f"reader_{i}"
        sid = f"cs_multi_{i}"
        events += [
            ev("Page Viewed",  {"page_url": f"https://media.com/article/{i}", "session_id": sid}, user_id=uid),
            ev("Article Read", {"article_id": f"art_{i}", "title": f"Story {i}",
                                "duration_seconds": 100 + i * 30, "scroll_depth_pct": 70}, user_id=uid),
        ]
    return events


# ─────────────────────────────────────────────
# TEST REGISTRY
# ─────────────────────────────────────────────

ECOM_SCENARIOS = [
    ("happy path",                ecom_happy_path,               []),
    ("refund after purchase",     ecom_refund_after_purchase,    []),
    ("coupon valid",              ecom_coupon_valid,             []),
    ("multi-user clean",          ecom_multi_user_session,       []),
    ("wishlist no login",         ecom_wishlist_no_login,        ["wishlist_without_identity"]),
    ("purchase without checkout", ecom_purchase_without_checkout,["purchase_without_checkout"]),
    ("duplicate purchase",        ecom_duplicate_purchase,       ["duplicate_purchase"]),
    ("bad currency",              ecom_bad_currency,             ["invalid_currency"]),
    ("revenue as string",         ecom_revenue_as_string,        ["wrong_property_type"]),
    ("missing required prop",     ecom_missing_required_prop,    ["missing_property"]),
    ("refund without purchase",   ecom_refund_without_purchase,  ["return_without_purchase"]),
    ("checkout step regression",  ecom_checkout_step_regression, ["checkout_step_regression"]),
    ("coupon without checkout",   ecom_coupon_without_checkout,  ["coupon_without_checkout"]),
    ("unknown event",             ecom_unknown_event,            ["unknown_event"]),
    ("duplicate event id",        ecom_duplicate_event_id,       ["duplicate_event"]),
]

SAAS_SCENARIOS = [
    ("happy path",                 saas_happy_path,                  []),
    ("cancel after subscription",  saas_cancel_after_subscription,   []),
    ("multi-user clean",           saas_multi_user,                  []),
    ("login before signup",        saas_login_before_signup,         ["sequence_error"]),
    ("trial missing identity",     saas_trial_missing_identity,      ["missing_identity"]),
    ("invalid plan enum",          saas_invalid_plan_enum,           ["enum_value_violation"]),
    ("invalid login method",       saas_invalid_login_method,        ["enum_value_violation"]),
    ("subscription missing mrr",   saas_subscription_missing_mrr,    ["missing_property"]),
    ("subscription without trial", saas_subscription_without_trial,  ["sequence_error"]),
    ("mrr as boolean",             saas_mrr_as_boolean,              ["wrong_property_type"]),
    ("unknown event",              saas_unknown_event,               ["unknown_event"]),
]

CONTENT_SCENARIOS = [
    ("happy path",                   content_happy_path,                   []),
    ("subscription valid",           content_subscription,                 []),
    ("search anonymous clean",       content_search_no_identity,           []),
    ("multi-user clean",             content_multi_user,                   []),
    ("article before page view",     content_article_before_page_view,     ["sequence_error"]),
    ("video complete without start", content_video_complete_without_start, ["sequence_error"]),
    ("bad share channel",            content_bad_share_channel,            ["enum_value_violation"]),
    ("duration as string",           content_duration_as_string,           ["wrong_property_type"]),
    ("missing scroll depth",         content_missing_scroll_depth,         ["missing_property"]),
    ("bad bookmark type",            content_bad_bookmark_type,            ["enum_value_violation"]),
    ("invalid subscription plan",    content_invalid_subscription_plan,    ["enum_value_violation"]),
    ("watch pct missing",            content_watch_pct_missing,            ["missing_property"]),
]


# ─────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────

SEV_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"


def sev_color(sev):
    return {"critical": "\033[91m", "high": "\033[93m",
            "medium": "\033[94m", "low": "\033[92m"}.get(sev, RESET)


def run_suite(suite_name, plan_path, scenarios):
    raw_specs = load_tracking_plan(plan_path)
    specs = normalize_specs(raw_specs)
    profile = analyze_tracking_plan(specs)

    print(f"\n{'='*70}")
    print(f"{BOLD}{CYAN}  {suite_name.upper()} PLAN{RESET}")
    print(f"{'='*70}")
    print(f"  Events in plan : {len(specs)}")
    print(f"  Domain detected: {profile.domain}  (confidence {profile.confidence:.2f})")
    print(f"  Funnel map     : {', '.join(f'{k}={v}' for k,v in profile.funnel_map.items()) or 'none'}")
    print(f"  Property map   : {profile.property_map}")
    print(f"  Checks enabled : {', '.join(sorted(profile.enabled_checks))}")
    print()

    total = passed = failed = 0
    fail_details = []

    for label, fn, expected_types in scenarios:
        total += 1
        events = fn()
        state = make_state_from_profile(profile)
        issues = detect_issues(
            events, specs,
            enabled_checks=profile.enabled_checks,
            state=state,
            funnel_map=profile.funnel_map,
            property_map=profile.property_map,
        )

        found_types = {i.issue_type for i in issues}

        if not expected_types:
            ok = len(issues) == 0
        else:
            ok = all(t in found_types for t in expected_types)

        if ok:
            passed += 1
            status = f"{GREEN}PASS{RESET}"
        else:
            failed += 1
            status = f"{RED}FAIL{RESET}"
            fail_details.append((label, expected_types, issues))

        issue_summary = ""
        if issues:
            by_sev = {}
            for i in issues:
                by_sev.setdefault(i.severity, 0)
                by_sev[i.severity] += 1
            parts = [f"{sev_color(s)}{c} {s}{RESET}"
                     for s, c in sorted(by_sev.items(), key=lambda x: SEV_ORDER.get(x[0], 9))]
            issue_summary = "  issues: " + ", ".join(parts)

        print(f"  [{status}]  {label:<38}{issue_summary}")

        if issues:
            for iss in sorted(issues, key=lambda i: SEV_ORDER.get(i.severity, 9)):
                msg = iss.message[:88]
                print(f"           {DIM}{sev_color(iss.severity)}[{iss.severity}]{RESET}"
                      f"{DIM} {iss.issue_type}: {msg}{RESET}")

    print(f"\n  {BOLD}Results: {passed}/{total} passed{RESET}", end="")
    if failed:
        print(f"  {RED}{failed} FAILED{RESET}")
        for label, expected, issues in fail_details:
            found = {i.issue_type for i in issues}
            missing = set(expected) - found
            unexpected = found - set(expected) if expected else found
            print(f"\n    {RED}x FAIL: {label}{RESET}")
            if missing:
                print(f"      Expected but NOT found : {missing}")
            if unexpected and expected:
                print(f"      Found but not expected : {unexpected}")
            if not expected and issues:
                print(f"      Expected CLEAN but got : {found}")
    else:
        print(f"  {GREEN}All clean{RESET}")

    return total, passed, failed


def main():
    results = []
    results.append(run_suite(
        "Ecommerce",
        "sample_data/tracking_plan_ecommerce.json",
        ECOM_SCENARIOS,
    ))
    results.append(run_suite(
        "SaaS",
        "sample_data/tracking_plan_saas.json",
        SAAS_SCENARIOS,
    ))
    results.append(run_suite(
        "Content",
        "sample_data/tracking_plan_content.json",
        CONTENT_SCENARIOS,
    ))

    grand_total  = sum(r[0] for r in results)
    grand_passed = sum(r[1] for r in results)
    grand_failed = sum(r[2] for r in results)

    print(f"\n{'='*70}")
    print(f"{BOLD}  GRAND TOTAL: {grand_passed}/{grand_total} scenarios passed", end="")
    if grand_failed:
        print(f"   {RED}{grand_failed} FAILED{RESET}")
    else:
        print(f"   {GREEN}ALL PASS{RESET}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
