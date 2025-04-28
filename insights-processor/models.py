# insights-processor/models.py

from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class StorageMetric:
    """Represents a storage metric document from MongoDB."""
    device_id: str
    timestamp: float
    iops: int
    latency: float
    capacity_used: float
    _id: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StorageMetric':
        """Create a StorageMetric from a dictionary."""
        return cls(
            device_id=data.get('device_id'),
            timestamp=data.get('timestamp'),
            iops=data.get('iops'),
            latency=data.get('latency'),
            capacity_used=data.get('capacity_used'),
            _id=str(data.get('_id')) if data.get('_id') else None
        )


@dataclass
class DeviceInsight:
    """Represents processed insights for a storage device."""
    device_id: str
    period_start: datetime
    period_end: datetime
    avg_iops: float
    avg_latency: float
    avg_capacity_used: float
    max_iops: float
    max_latency: float
    max_capacity_used: float
    min_iops: float
    min_latency: float
    min_capacity_used: float
    metrics_count: int
    alerts: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format for database storage."""
        return {
            'device_id': self.device_id,
            'period_start': self.period_start,
            'period_end': self.period_end,
            'avg_iops': self.avg_iops,
            'avg_latency': self.avg_latency,
            'avg_capacity_used': self.avg_capacity_used,
            'max_iops': self.max_iops,
            'max_latency': self.max_latency,
            'max_capacity_used': self.max_capacity_used,
            'min_iops': self.min_iops,
            'min_latency': self.min_latency,
            'min_capacity_used': self.min_capacity_used,
            'metrics_count': self.metrics_count,
            'alerts': self.alerts,
        }