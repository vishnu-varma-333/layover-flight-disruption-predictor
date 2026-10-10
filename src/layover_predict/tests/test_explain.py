from layover_predict.explain import explain


def test_ranks_by_contribution_magnitude_not_declaration_order():
    row = {"prev_arr_delay_min": 50.0, "turnaround_minutes": 90.0, "tight_turnaround": False}
    contributions = {"prev_arr_delay_min": 2.0, "turnaround_minutes": 15.0}
    sentences = explain(row, contributions, top_k=1)
    assert len(sentences) == 1
    assert "turnaround" in sentences[0]  # the bigger contribution, despite being declared second


def test_skips_a_feature_with_no_known_value_even_if_it_has_a_contribution():
    # LightGBM assigns every feature a (near-zero) contribution via its
    # missing-value handling, even when the row's actual value is None --
    # that's not evidence of anything and must not surface as a sentence.
    row = {"prev_arr_delay_min": None, "cum_delay_today": 30.0}
    contributions = {"prev_arr_delay_min": 0.001, "cum_delay_today": 5.0}
    sentences = explain(row, contributions, top_k=2)
    assert len(sentences) == 1
    assert "30" in sentences[0]


def test_has_inbound_explainer_only_fires_when_false():
    row_no_inbound = {"has_inbound": False}
    assert explain(row_no_inbound, {"has_inbound": 3.0}, top_k=1) != []

    row_with_inbound = {"has_inbound": True}
    assert explain(row_with_inbound, {"has_inbound": 3.0}, top_k=1) == []


def test_origin_and_dest_collapse_into_a_single_route_sentence():
    row = {"origin": "DEN", "dest": "ORD"}
    sentences = explain(row, {"origin": 4.0, "dest": 3.0}, top_k=3)
    assert len(sentences) == 1
    assert "DEN-ORD" in sentences[0]


def test_unknown_feature_in_contributions_is_silently_skipped():
    row = {"leg_index": 2, "prev_dep_delay_min": 10.0}
    # Neither "leg_index" nor "prev_dep_delay_min" has a dedicated
    # explainer -- both should be skipped, not raise.
    assert explain(row, {"leg_index": 9.0, "prev_dep_delay_min": 8.0}, top_k=3) == []


def test_negative_turnaround_reads_as_overdue_not_tight():
    # Found live: a genuinely late inbound aircraft can push
    # turnaround_minutes negative (its landing is already past this
    # flight's own scheduled departure). `tight_turnaround` is
    # technically true there too (< 45 min), but "a tight turnaround"
    # reads as reassuring for what's actually an overdue aircraft --
    # abs() in the generic formatter would also hide the sign entirely.
    row = {"turnaround_minutes": -463.0, "tight_turnaround": True}
    sentences = explain(row, {"turnaround_minutes": 260.0}, top_k=1)
    assert len(sentences) == 1
    assert "overdue" in sentences[0]
    assert "tight turnaround" not in sentences[0]
    assert "463" in sentences[0]


def test_top_k_caps_the_number_of_sentences():
    row = {
        "prev_arr_delay_min": 10.0,
        "turnaround_minutes": 20.0,
        "tight_turnaround": False,
        "cum_delay_today": 30.0,
        "dep_delay_min": 5.0,
    }
    contributions = {
        "prev_arr_delay_min": 1.0,
        "turnaround_minutes": 2.0,
        "cum_delay_today": 3.0,
        "dep_delay_min": 4.0,
    }
    assert len(explain(row, contributions, top_k=2)) == 2
