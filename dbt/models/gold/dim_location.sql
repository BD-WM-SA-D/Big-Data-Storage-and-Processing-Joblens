-- gold.dim_location – contract joblens/schemas/tables/gold/dim_location.yaml 0.1.0
-- Nguồn: seeds/locations_2025.csv (Quyết định 19/2025/QĐ-TTg, Nghị quyết 202/2025/QH15 – xem seeds/README.md)
select
    cast(location_code as varchar) as location_code,
    cast(name as varchar) as name,
    cast(region as varchar) as region,
    split(cast(aliases as varchar), '|') as aliases
from {{ ref('locations_2025') }}
