import pandas as pd
from src.s04_results_analyzer import calculate_test
from src.s03_analysis_preparator import get_test_times
from datetime import datetime, timedelta

def test_calculate_test():
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
    
    df.set_index('timeStamp', inplace=True)
    df.index = pd.to_datetime(df.index)
    
    test_start_time: datetime = df.index.min()
    test_end_time: datetime = df.index.max() + timedelta(seconds=1)
    full_test_duration_seconds = int((test_end_time - test_start_time).total_seconds())
    ramp_up_time_seconds = 60
    impact_time_seconds = 240
    ranges_count = 1
    duration_range_seconds = 240
    ramp_down_time_seconds = 30    
    
    test_times = get_test_times(test_start_time, test_end_time, full_test_duration_seconds, ramp_up_time_seconds, impact_time_seconds, ranges_count, duration_range_seconds, ramp_down_time_seconds)
    unique_labels = ['protocol, host, port', 'POST_/user/v1/systemuser/authtoken', 'jedisPool_Redis', 'redisDB_merchants', 'props.put', 'protocol, host, port', 'POST_/user/v1/systemuser/authtoken', 'jedisPool_Redis', 'redisDB_merchants', 'props.put', 'protocol, host, port', 'POST_/user/v1/systemuser/authtoken']
    freq = '30s'
    
    descriptive_analysis_results = calculate_test(df, test_times, unique_labels, freq)
        
    actual_sampler_count = descriptive_analysis_results['full_test']['summary_range_results']['sampler_count']
    expected_sampler_count = len(data['responseCode'])
    
    assert actual_sampler_count == expected_sampler_count