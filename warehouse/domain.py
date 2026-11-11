from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT a.event, i.subject, a.category FROM activity a LEFT JOIN identities i ON a.alias=i.alias ORDER BY a.event')]
    return rows
