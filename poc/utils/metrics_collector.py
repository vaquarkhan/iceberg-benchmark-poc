#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright 2026 Vaquar Khan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

================================================================================
Metrics Collector
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Collect JVM and system metrics for performance analysis
"""

import gc
import time
import psutil
import os
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional


@dataclass
class GCMetrics:
    """Garbage collection metrics"""
    timestamp: float
    collection_count: int
    pause_time_ms: float
    heap_usage_mb: float
    object_count: int
    
    def to_dict(self):
        return asdict(self)


@dataclass
class SystemMetrics:
    """System resource metrics"""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    
    def to_dict(self):
        return asdict(self)


class MetricsCollector:
    """Collects performance metrics"""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.gc_metrics: List[GCMetrics] = []
        self.system_metrics: List[SystemMetrics] = []
        
    def collect_gc_metrics(self) -> GCMetrics:
        """Collect current GC metrics"""
        gc.collect()
        stats = gc.get_stats()
        
        total_collections = sum(s.get('collections', 0) for s in stats)
        total_objects = len(gc.get_objects())
        
        # Estimate pause time
        estimated_pause_ms = (total_objects / 10000) * 0.1
        
        memory_info = self.process.memory_info()
        heap_usage_mb = memory_info.rss / 1024 / 1024
        
        metrics = GCMetrics(
            timestamp=time.time(),
            collection_count=total_collections,
            pause_time_ms=estimated_pause_ms,
            heap_usage_mb=heap_usage_mb,
            object_count=total_objects
        )
        
        self.gc_metrics.append(metrics)
        return metrics
    
    def collect_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        memory_info = self.process.memory_info()
        
        metrics = SystemMetrics(
            timestamp=time.time(),
            cpu_percent=self.process.cpu_percent(),
            memory_mb=memory_info.rss / 1024 / 1024,
            memory_percent=self.process.memory_percent()
        )
        
        self.system_metrics.append(metrics)
        return metrics
    
    def get_summary(self) -> Dict:
        """Get summary of collected metrics"""
        if not self.gc_metrics:
            return {}
        
        return {
            'gc': {
                'total_collections': self.gc_metrics[-1].collection_count,
                'avg_pause_ms': sum(m.pause_time_ms for m in self.gc_metrics) / len(self.gc_metrics),
                'max_pause_ms': max(m.pause_time_ms for m in self.gc_metrics),
                'avg_heap_mb': sum(m.heap_usage_mb for m in self.gc_metrics) / len(self.gc_metrics)
            },
            'system': {
                'avg_cpu_percent': sum(m.cpu_percent for m in self.system_metrics) / len(self.system_metrics) if self.system_metrics else 0,
                'avg_memory_mb': sum(m.memory_mb for m in self.system_metrics) / len(self.system_metrics) if self.system_metrics else 0
            }
        }
    
    def reset(self):
        """Reset collected metrics"""
        self.gc_metrics = []
        self.system_metrics = []
