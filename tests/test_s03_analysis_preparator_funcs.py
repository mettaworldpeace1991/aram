import pandas as pd
from datetime import datetime, timedelta
from src.s03_analysis_preparator import get_unique_labels, get_test_times

def test_get_unique_labels():
    data = {'label': ['A', 'B', 'A', 'C']}
    df = pd.DataFrame(data)
    result = get_unique_labels(df)
    assert result == ['A', 'B', 'C']

def test_get_test_times():
    data = {
        'timeStamp': ['2024-01-11 05:46:41.610', '2024-01-11 05:47:11.825', '2024-01-11 05:47:41.127', '2024-01-11 05:48:11.227', '2024-01-11 05:48:41.333', '2024-01-11 05:49:11.520', '2024-01-11 05:49:41.730', '2024-01-11 05:50:11.018', '2024-01-11 05:50:41.109', '2024-01-11 05:51:11.212', '2024-01-11 05:51:41.074', '2024-01-11 05:52:11.268'],
        'elapsed': [179, 262, 100, 106, 26, 175, 247, 90, 102, 29, 160, 260],
        'label': ['protocol, host, port', 'POST_/user/v1/systemuser/authtoken', 'jedisPool_Redis', 'redisDB_merchants', 'props.put', 'protocol, host, port', 'POST_/user/v1/systemuser/authtoken', 'jedisPool_Redis', 'redisDB_merchants', 'props.put', 'protocol, host, port', 'POST_/user/v1/systemuser/authtoken'],
        'responseCode': [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200],
        'threadName': ['setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1', 'setUp TG (Auth and Get Token) 1-1'],
        'success': [True, True, True, True, True, True, True, True, True, True, True, True],
        'Latency': [0, 261, 0, 0, 0, 0, 246, 0, 0, 0, 0, 259],
        'Connect': [0, 145, 0, 0, 0, 0, 141, 0, 0, 0, 0, 146]
    }
    
    df = pd.DataFrame(data)
    
    test_start_time: datetime = df['timeStamp'].apply(pd.Timestamp).min()
    test_end_time: datetime = df['timeStamp'].apply(pd.Timestamp).max()
    full_test_duration_seconds = int((test_end_time - test_start_time).total_seconds())
    ramp_up_time_seconds = 60
    impact_time_seconds = 240
    ranges_count = 1
    duration_range_seconds = 240
    ramp_down_time_seconds = 30    
    
    test_times = get_test_times(test_start_time, test_end_time, full_test_duration_seconds, ramp_up_time_seconds, impact_time_seconds, ranges_count, duration_range_seconds, ramp_down_time_seconds)
    
    expected_test_start_date = test_start_time
    expected_ramp_up_end_date = expected_test_start_date + timedelta(seconds=int(ramp_up_time_seconds))
    expected_impact_start_date = expected_ramp_up_end_date
    expected_impact_end_date = expected_impact_start_date + timedelta(seconds=int(impact_time_seconds))
    expected_assessment_ranges_count = ranges_count
    expected_ramp_down_start_date = expected_impact_end_date
    expected_test_end_date = expected_ramp_down_start_date + timedelta(seconds=int(ramp_down_time_seconds))
    
    assert expected_test_start_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.ramp_up.start_time.iso
    assert expected_ramp_up_end_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.ramp_up.end_time.iso
    assert expected_impact_start_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.impact.start_time.iso
    assert expected_impact_end_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.impact.end_time.iso
    assert expected_ramp_down_start_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.ramp_down.start_time.iso
    assert expected_test_end_date.strftime('%Y-%m-%dT%H:%M:%S') == test_times.full_test.end_time.iso
    assert expected_assessment_ranges_count == len(test_times.duration_ranges)