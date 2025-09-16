# pcs_v3 package: centralizes Pancake v3 discovery, math, quoting, and utils.

from .utils import (
    QPSLimiter, limited_call, make_web3, to_checksum,
    CALL_RETRIES, HTTP_TIMEOUT, limiter, load_tokens,
)
from .discovery import build_rate_matrix_v3
from .math import pretty_print_matrix, scan_triangular_opps, estimate_usd_price_for_symbol
from .quote import (
    encode_path, quoter_amount_out_single, quoter_amount_out_path,
    simulate_path_usd, router_estimate_gas_exact_input
)
