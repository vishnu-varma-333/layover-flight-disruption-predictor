from layover_trip.risk import connection_p_miss, p_exceeds


def test_p_exceeds_is_monotonically_non_increasing_in_threshold():
    # Higher threshold -> harder to exceed -> lower or equal probability.
    values = [p_exceeds(t, "rotation") for t in (-50, -10, 0, 10, 50, 150)]
    assert all(a >= b for a, b in zip(values, values[1:]))


def test_p_exceeds_median_residual_is_roughly_fifty_percent():
    # The measured p50 residual quantile is ~0.6 min for the rotation
    # model -- P(residual > median) should be close to 0.5 by
    # definition of a median.
    assert abs(p_exceeds(0.6, "rotation") - 0.5) < 0.02


def test_p_exceeds_clamped_to_zero_and_one():
    assert p_exceeds(-10_000, "rotation") == 1.0
    assert p_exceeds(10_000, "rotation") == 0.0


def test_connection_p_miss_zero_buffer_with_zero_predicted_delay_is_the_median_case():
    # No slack, no predicted delay: missing the connection means the
    # real delay exceeds 0 -- roughly a coin flip given delay's
    # near-zero median, not a near-certainty or near-impossibility.
    p = connection_p_miss(predicted_delay=0.0, model_name="rotation", buffer_minutes=0.0)
    assert 0.3 < p < 0.7


def test_connection_p_miss_large_predicted_delay_against_small_buffer_is_high_risk():
    p = connection_p_miss(predicted_delay=100.0, model_name="post_departure", buffer_minutes=7.0)
    assert p > 0.9


def test_connection_p_miss_large_buffer_against_small_predicted_delay_is_low_risk():
    p = connection_p_miss(predicted_delay=5.0, model_name="post_departure", buffer_minutes=120.0)
    assert p < 0.1


def test_post_departure_has_tighter_distribution_than_rotation():
    # The post-departure model already knows this flight's own departure
    # delay, so its residual distribution should be tighter -- the same
    # threshold should look less risky under it than under the
    # pre-departure rotation model's wider uncertainty.
    rotation_p = p_exceeds(20, "rotation")
    post_departure_p = p_exceeds(20, "post_departure")
    assert post_departure_p <= rotation_p
