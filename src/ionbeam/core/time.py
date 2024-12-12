from datetime import datetime, timedelta


def round_datetime(dt: datetime, round_to: timedelta, method: str = "floor") -> datetime:
    if round_to.total_seconds() <= 0:
        raise ValueError("round_to must represent a positive duration")
    if method not in {"floor", "ceil"}:
        raise ValueError("method must be 'floor' or 'ceil'")
    
    # Calculate the number of seconds since the start of the day
    total_seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

    # Get the total number of seconds for the rounding interval
    rounding_seconds = round_to.total_seconds()

    if method == "floor":
        rounded_seconds = (total_seconds // rounding_seconds) * rounding_seconds
    else:  # method == "ceil"
        rounded_seconds = ((total_seconds + rounding_seconds - 1) // rounding_seconds) * rounding_seconds
    
    # Return the rounded datetime
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=rounded_seconds)

def split_time_interval_into_chunks(start: datetime, end: datetime, chunk_size: timedelta) -> list[datetime]:
        start = round_datetime(start, round_to = chunk_size, method="floor")
        end = round_datetime(end, round_to = chunk_size, method="ceil")
        
        n_points = int((end - start) / chunk_size)
        return [start + i * chunk_size for i in range(n_points + 1)] # +1 to include the end date
