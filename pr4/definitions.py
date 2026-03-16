from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import lab_renta
import interactive_map
import lab_renta_checks

defs = Definitions(
    assets=load_assets_from_modules([lab_renta, interactive_map]),
    asset_checks=load_asset_checks_from_modules([lab_renta_checks])
)