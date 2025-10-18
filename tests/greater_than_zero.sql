select *
from {{ ref('fct_claims') }}
where loss_amount < 0 or total_payout_amount < 0
