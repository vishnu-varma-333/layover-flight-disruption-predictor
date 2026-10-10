"""Turns a model's per-feature contributions into the evidence sentences
the spec asks for ("your aircraft is 50 minutes late leaving Denver").

Contributions come from LightGBM's native `pred_contrib` (exact,
TreeSHAP-consistent attribution of this specific prediction to each
input feature) -- the tech-stack table's own stated reason for choosing
LightGBM over alternatives is "interpretable feature contributions for
explanations," so this is the mechanism that line of the spec is
actually pointing at, not a heuristic built on top of it.

Each explainer below only fires when its feature has a genuine, known
value -- a None/NaN feature still gets a (near-zero) contribution from
LightGBM's missing-value handling, which isn't evidence of anything and
would read as a non-sequitur if turned into a sentence.
"""

DIRECTION_INCREASE = "pushing predicted delay up"
DIRECTION_DECREASE = "pulling predicted delay down"


def _direction(contribution: float) -> str:
    return DIRECTION_INCREASE if contribution > 0 else DIRECTION_DECREASE


def _minutes(x: float) -> str:
    return f"{abs(x):.0f} min"


def _lateness_word(x: float) -> str:
    return "late" if x > 0 else "early" if x < 0 else "on time"


def _explain_prev_arr_delay_min(row, contribution):
    val = row.get("prev_arr_delay_min")
    if val is None:
        return None
    return (
        f"Inbound aircraft's previous leg landed {_minutes(val)} {_lateness_word(val)} "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_turnaround_minutes(row, contribution):
    val = row.get("turnaround_minutes")
    if val is None:
        return None
    # A negative value means the inbound aircraft's actual landing is
    # already past this flight's own scheduled departure -- a genuinely
    # different, more alarming situation than an ordinary short gap, and
    # `abs()` in `_minutes()` would otherwise hide the sign entirely.
    # `tight_turnaround` (< 45 min, per training) is also technically
    # true here, but "a tight turnaround" reads as reassuring in a way
    # the actual number ("already overdue") is not -- found live, not
    # assumed, see DECISIONS.md.
    if val < 0:
        return (
            f"Scheduled turnaround is already overdue by {_minutes(val)} -- the inbound "
            f"aircraft landed later than this flight's own scheduled departure "
            f"({_direction(contribution)} by {_minutes(contribution)})."
        )
    tight = " -- a tight turnaround" if row.get("tight_turnaround") else ""
    return (
        f"Scheduled turnaround is {_minutes(val)}{tight} "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_cum_delay_today(row, contribution):
    val = row.get("cum_delay_today")
    if val is None:
        return None
    return (
        f"This aircraft has accumulated {_minutes(val)} of delay earlier today "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_dep_delay_min(row, contribution):
    val = row.get("dep_delay_min")
    if val is None:
        return None
    return (
        f"This flight already departed {_minutes(val)} {_lateness_word(val)} "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_chain_break(row, contribution):
    if not row.get("chain_break"):
        return None
    return (
        f"This aircraft flew in from a different airport than its previous leg's "
        f"destination (an equipment swap) ({_direction(contribution)} by "
        f"{_minutes(contribution)})."
    )


def _explain_has_inbound(row, contribution):
    if row.get("has_inbound"):
        return None  # the more specific rotation explainers above cover this case
    return (
        f"This is the aircraft's first leg of the day -- no inbound delay to "
        f"inherit ({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_dep_hour_local(row, contribution):
    val = row.get("dep_hour_local")
    if val is None:
        return None
    return (
        f"Scheduled to depart at {val:02d}:00 local time "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_day_of_week(row, contribution):
    val = row.get("day_of_week")
    if val is None:
        return None
    day = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][val]
    return (
        f"Historical {day} congestion patterns "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_month(row, contribution):
    if row.get("month") is None:
        return None
    return (
        f"Seasonal delay patterns for this time of year "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_route(row, contribution):
    origin, dest = row.get("origin"), row.get("dest")
    if origin is None or dest is None:
        return None
    return (
        f"Historical delay patterns on the {origin}-{dest} route "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


def _explain_carrier(row, contribution):
    carrier = row.get("carrier")
    if carrier is None:
        return None
    return (
        f"{carrier}'s historical on-time performance "
        f"({_direction(contribution)} by {_minutes(contribution)})."
    )


_EXPLAINERS = {
    "prev_arr_delay_min": _explain_prev_arr_delay_min,
    "turnaround_minutes": _explain_turnaround_minutes,
    "cum_delay_today": _explain_cum_delay_today,
    "dep_delay_min": _explain_dep_delay_min,
    "chain_break": _explain_chain_break,
    "has_inbound": _explain_has_inbound,
    "dep_hour_local": _explain_dep_hour_local,
    "day_of_week": _explain_day_of_week,
    "month": _explain_month,
    "origin": _explain_route,
    "dest": _explain_route,
    "carrier": _explain_carrier,
}


def explain(row: dict, contributions: dict, top_k: int = 3) -> list:
    """Up to `top_k` evidence sentences, ranked by the magnitude of each
    feature's actual contribution to *this* prediction -- not a fixed
    feature-importance ranking, which would be identical for every
    flight and defeat the point of a per-flight explanation.
    """
    ranked = sorted(contributions.items(), key=lambda kv: -abs(kv[1]))
    sentences = []
    seen_routes = False
    for feature, contribution in ranked:
        if feature in ("origin", "dest"):
            if seen_routes:
                continue
            seen_routes = True
        explainer = _EXPLAINERS.get(feature)
        if explainer is None:
            continue
        sentence = explainer(row, contribution)
        if sentence is not None:
            sentences.append(sentence)
        if len(sentences) >= top_k:
            break
    return sentences
