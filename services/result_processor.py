import random
import uuid
from collections import Counter
from typing import List, Dict, Any
from services.parquet_manager import save_to_parquet

class ResultProcessor:
    FULL_THRESHOLD = 100
    MAX_VALUE_LENGTH = 300

    def process(self, results: List[Dict], total_count: int, hint: str) -> Dict:
        if not results:
            return {
                "status": "success",
                "mode": "empty",
                "total_count": total_count,
                "message": "The query returned no results."
            }

        n = len(results)
        clean_results = self._truncate(results)

        if hint == "aggregate_only" or n > self.FULL_THRESHOLD:
            # Route large payloads to Parquet
            cache_id = f"cache_{uuid.uuid4().hex[:8]}"
            save_to_parquet(cache_id, clean_results)
            
            # Send Data Profile (Observation) to LLM
            head = clean_results[:5]
            return {
                "status": "success",
                "mode": "cached_profile",
                "rows_retrieved": n,
                "total_count_in_db": total_count,
                "cache_id": cache_id,
                "preview": head,
                "statistics": self._compute_stats(clean_results),
                "message": f"Result too large ({n} rows). Saved to cache. Use ANALYZE_CACHE action with cache_id '{cache_id}' and DuckDB SQL."
            }

        return {
            "status": "success",
            "mode": "full",
            "data": clean_results,
            "row_count": n,
            "total_count": total_count
        }

    def _compute_stats(self, results: List[Dict]) -> Dict:
        if not results:
            return {}
        stats = {}
        for key in results[0].keys():
            values = [r[key] for r in results if r.get(key) is not None]
            null_count = len(results) - len(values)
            if not values:
                stats[key] = {"null_count": null_count}
                continue
            if all(isinstance(v, (int, float)) for v in values):
                stats[key] = {
                    "min": min(values),
                    "max": max(values),
                    "avg": round(sum(values) / len(values), 3),
                    "sum": sum(values),
                    "null_count": null_count
                }
            else:
                counter = Counter(str(v)[:50] for v in values)
                stats[key] = {
                    "unique_count": len(counter),
                    "top_5": dict(counter.most_common(5)),
                    "null_count": null_count
                }
        return stats

    def _truncate(self, results: List[Dict]) -> List[Dict]:
        truncated = []
        for row in results:
            new_row = {}
            for k, v in row.items():
                if isinstance(v, str) and len(v) > self.MAX_VALUE_LENGTH:
                    new_row[k] = v[:self.MAX_VALUE_LENGTH] + "...[truncated]"
                else:
                    new_row[k] = v
            truncated.append(new_row)
        return truncated

result_processor = ResultProcessor()
