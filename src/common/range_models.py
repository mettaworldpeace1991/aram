import numpy as np
from enum import Enum
from typing import List, Dict
from json import JSONEncoder
from datetime import datetime
from dataclasses import asdict, is_dataclass, dataclass

class GBEncoder(JSONEncoder):
    """
    A custom JSON encoder to handle datetime objects.
    """

    @staticmethod
    def default(o):
        """
        Overrides the default method to provide custom serialization for datetime objects.
        :param o: The object to serialize.
        :return: A serializable representation of the object.
        """
        if hasattr(o, 'to_dict'):
            return o.to_dict()
        if isinstance(o, datetime):
            serialized_obj = {
                'year': o.year,
                'month': o.month,
                'day': o.day,
                'hour': o.hour,
                'minute': o.minute,
                'second': o.second,
                'isoformat': o.isoformat(),
            }
            return serialized_obj
        elif isinstance(o, Enum):
            return o.value
        elif is_dataclass(o):
            return asdict(o)
        elif isinstance(o, np.int64):
            serialized_obj = int(o)
            return serialized_obj
        else:
            return super(GBEncoder, GBEncoder).default(o)

@dataclass
class TimeFormat():
    def __init__(self, dt: datetime):
        self.epoch: int = int(dt.timestamp() * 1e9)
        self.iso: str = dt.strftime('%Y-%m-%dT%H:%M:%S')
        self.date: str = dt.strftime('%Y-%m-%d')
        self.time: str = dt.strftime('%H:%M:%S')

    def to_dict(self):
        return {
            'epoch': self.epoch,
            'iso': self.iso,
            'date': self.date,
            'time': self.time
        }

    @classmethod
    def from_dict(cls, time_data: Dict):
        timestamp_ns = time_data.get('epoch')
        timestamp_s = timestamp_ns / 1e9
        dt = datetime.fromtimestamp(timestamp_s)
        return cls(dt)

@dataclass
class TimeRange():
    def __init__(self, duration_in_seconds: int,
                full_range_name: str, short_range_name: str,
                start_time: TimeFormat, end_time: TimeFormat
                ):
        self.duration_in_seconds: int = duration_in_seconds
        self.full_range_name: str = full_range_name
        self.short_range_name: str = short_range_name
        self.start_time: TimeFormat = start_time
        self.end_time: TimeFormat = end_time

    def to_dict(self):
        return {
            'duration_in_seconds': self.duration_in_seconds,
            'full_range_name': self.full_range_name,
            'short_range_name': self.short_range_name,
            'start_time': self.start_time.to_dict(),
            'end_time': self.end_time.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict):
        start_time_data = data.get('start_time', {})
        start_time = TimeFormat.from_dict(start_time_data)

        end_time_data = data.get('end_time', {})
        end_time = TimeFormat.from_dict(end_time_data)

        return cls(
            duration_in_seconds=int(data.get('duration_in_seconds', 0)),
            full_range_name=data.get('full_range_name', ''),
            short_range_name=data.get('short_range_name', ''),
            start_time=start_time,
            end_time=end_time
        )

class AssessmentRange(TimeRange):
    def __init__(self,
                range_number: int,
                duration_in_seconds: int,
                full_range_name: str,
                short_range_name: str,
                start_time: TimeFormat,
                end_time: TimeFormat
        ):
        super().__init__(duration_in_seconds, full_range_name,
                        short_range_name, start_time, end_time)
        self.range_number = range_number

    def to_dict(self):
        return {
            'duration_in_seconds': self.duration_in_seconds,
            'full_range_name': self.full_range_name,
            'short_range_name': self.short_range_name,
            'range_number': self.range_number,
            'start_time': self.start_time.to_dict(),
            'end_time': self.end_time.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict):
        start_time_data = data.get('start_time', {})
        start_time = TimeFormat.from_dict(start_time_data)

        end_time_data = data.get('end_time', {})
        end_time = TimeFormat.from_dict(end_time_data)

        return cls(
            duration_in_seconds=int(data.get('duration_in_seconds', 0)),
            full_range_name=data.get('full_range_name', ''),
            short_range_name=data.get('short_range_name', ''),
            range_number=data.get('range_number', ''),
            start_time=start_time,
            end_time=end_time
        )

@dataclass
class TestTimes():
    full_test: TimeRange
    ramp_up: TimeRange
    impact: TimeRange
    duration_ranges: List[AssessmentRange]
    ramp_down: TimeRange

    def get_all_ranges(self):
        all_ranges_list = [
            self.full_test,
            self.ramp_up,
            self.impact,
            self.ramp_down
        ]
        all_ranges_list.extend(self.duration_ranges)
        return all_ranges_list

    def to_dict(self):
        return {
            'full_test': self.full_test.to_dict(),
            'ramp_up': self.ramp_up.to_dict(),
            'impact': self.impact.to_dict(),
            'duration_ranges': [obj.to_dict() for obj in self.duration_ranges],
            'ramp_down': self.ramp_down.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict):
        full_test = TimeRange.from_dict(data.get('full_test', {}))
        ramp_up = TimeRange.from_dict(data.get('ramp_up', {}))
        impact = TimeRange.from_dict(data.get('impact', {}))
        duration_ranges = [AssessmentRange.from_dict(data) for data in data.get('duration_ranges', [])]
        ramp_down = TimeRange.from_dict(data.get('ramp_down', {}))
        return cls(full_test=full_test, ramp_up=ramp_up, impact=impact, duration_ranges=duration_ranges, ramp_down=ramp_down)
