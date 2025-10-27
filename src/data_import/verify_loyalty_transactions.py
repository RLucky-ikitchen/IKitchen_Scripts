from typing import Callable, Dict, List

import pandas as pd
from src.data_import.db import supabase, get_table, get_existing_orders
from src.utils import format_receipt_id


def _within_ten_percent(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False
    try:
        a_f = float(a)
        b_f = float(b)
    except (ValueError, TypeError):
        return False
    if a_f == 0 and b_f == 0:
        return True
    denom = max(abs(a_f), abs(b_f))
    if denom == 0:
        return False
    return abs(a_f - b_f) <= 0.10 * denom


def verify_loyalty_transactions(logger: Callable[[str], None] = print) -> Dict[str, object]:
    tx_table = get_table("transactions", False)

    # Fetch transactions missing order_id
    response = supabase.table(tx_table).select(
        "id, created_at, pos_receipt_id, order_id, bill_total"
    ).is_("order_id", None).execute()

    transactions: List[Dict] = response.data or []

    if not transactions:
        logger("No transactions without order_id found. Nothing to verify.")
        return {"matched": 0, "problematic": 0, "issues": []}

    # Build receipt_ids
    receipt_ids: List[str] = []
    tx_by_receipt: Dict[str, List[Dict]] = {}
    for tx in transactions:
        rid = format_receipt_id(tx.get("pos_receipt_id", ""), tx.get("created_at", ""))
        receipt_ids.append(rid)
        tx_by_receipt.setdefault(rid, []).append(tx)

    # Fetch corresponding orders
    orders_map = get_existing_orders(list(set(receipt_ids)), False)

    matched_count = 0
    problematic_count = 0
    issues: List[str] = []

    # orders table not directly needed beyond retrieval above

    for rid, tx_list in tx_by_receipt.items():
        order = orders_map.get(rid)
        if not order:
            for tx in tx_list:
                problematic_count += 1
                created_at_val = tx.get("created_at")
                dt = pd.to_datetime(created_at_val, errors="coerce")
                display_date = dt.strftime("%Y-%m-%d") if not pd.isna(dt) else str(created_at_val)
                issues.append(
                    f"No matching order for transaction pos_receipt_id={tx.get('pos_receipt_id')} date={display_date} -> {rid}"
                )
            continue

        order_id = order.get("order_id")
        order_total = order.get("total_amount")

        for tx in tx_list:
            # Update the transaction with the matched order_id
            try:
                update_filter = supabase.table(tx_table)
                # Prefer id if present to avoid accidental multi-row updates
                if tx.get("id") is not None:
                    update_filter = update_filter.eq("id", tx["id"]).is_("order_id", None)
                else:
                    update_filter = (
                        update_filter
                        .eq("pos_receipt_id", tx.get("pos_receipt_id"))
                        .eq("created_at", tx.get("created_at"))
                        .is_("order_id", None)
                    )

                update_filter.update({"order_id": order_id}).execute()
            except Exception as e:
                problematic_count += 1
                issues.append(
                    f"Failed to update transaction for {rid}: {e}"
                )
                continue

            matched_count += 1

            # Validate totals within 10%
            bill_total = tx.get("bill_total")
            if not _within_ten_percent(order_total, bill_total):
                problematic_count += 1
                issues.append(
                    f"Amount mismatch for {rid}: order_total={order_total}, bill_total={bill_total}"
                )

    logger(f"Matched transactions: {matched_count}")
    logger(f"Problematic transactions: {problematic_count}")
    if issues:
        logger("Issues:")
        for msg in issues:
            logger(f"- {msg}")

    return {"matched": matched_count, "problematic": problematic_count, "issues": issues}


