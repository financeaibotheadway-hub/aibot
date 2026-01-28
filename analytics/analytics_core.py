if _db_context_cache and (time.time() - _db_context_time < VALUES_CACHE_TTL):
    return _db_context_cache  # tuple(text, allowed_json, allowed_dict)

logger.info("♻️ Refreshing DB Context (Top values, last 90 days)...")

cols_to_scan = {
    REVENUE_TABLE_REF: [
        "event_type",
        "revenue_type",
        "platform",
        "app_name",
        "geo_country",
        "provider",
    ],
    COST_TABLE_REF: [
        "document_type",
        "legal_entity",
        "costrev_center_code",
        "source_code",
        "account_name",
    ],
}

# Пріоритетні date-колонки (щоб не брати випадковий TIMESTAMP)
preferred_date_cols = {
    REVENUE_TABLE_REF: ["date", "event_date", "order_date", "created_at", "event_timestamp"],
    COST_TABLE_REF: ["posting_date", "date", "transaction_date", "created_at"],
}

allowed = {}  # {"revenue_test_databot.geo_country": ["US", ...], ...}
lines = ["ВАЖЛИВО: Реальні значення (TOP) в базі. Для WHERE використовуй ТІЛЬКИ їх."]

for table, cols in cols_to_scan.items():
    try:
        schema_objs = get_table_schema(table)
        existing = {c["name"] for c in schema_objs}
    except Exception:
        continue

    date_col, date_type = _pick_best_date_col(schema_objs, preferred_date_cols.get(table, []))
    date_expr = _date_filter_expr(date_col, date_type) if date_col else None

    table_short = table.split(".")[-1]

    for col in cols:
        if col not in existing:
            continue

        try:
            where_parts = [f"{col} IS NOT NULL"]
            if date_expr:
                where_parts.append(
                    f"{date_expr} >= DATE_SUB(CURRENT_DATE('{LOCAL_TZ}'), INTERVAL 90 DAY)"
                )
            where_sql = " AND ".join(where_parts)

            # TOP значення за частотою (краще ніж DISTINCT LIMIT 500)
            query = f"""
                SELECT
                  CAST({col} AS STRING) AS v,
                  COUNT(1) AS cnt
                FROM `{table}`
                WHERE {where_sql}
                GROUP BY v
                ORDER BY cnt DESC, v ASC
                LIMIT 30
            """
            job = bq_client.query(query)
            values = [str(r["v"]) for r in job.result() if r["v"] is not None]

            key = f"{table_short}.{col}"
            if values:
                allowed[key] = values
                # коротка версія в тексті
                preview = ", ".join([f"'{x}'" for x in values[:12]])
                lines.append(f"- {key}: {preview}" + (" ..." if len(values) > 12 else ""))

        except Exception as e:
            logger.warning(f"⚠️ Context fetch error for {table}.{col}: {e}")

allowed_json = json.dumps(allowed, ensure_ascii=False)
context_text = "\n".join(lines)

_db_context_cache = (context_text, allowed_json, allowed)
_db_context_time = time.time()
return _db_context_cache
