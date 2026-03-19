from dagster import (
    Definitions, 
    load_assets_from_modules, 
    load_asset_checks_from_modules,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    DefaultSensorStatus
)
import os, glob
import lab_renta
import interactive_map
import lab_renta_checks

# Definimos el Job que materializa todos los assets del proyecto
all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.all()
)

@sensor(job=all_assets_job, default_status=DefaultSensorStatus.RUNNING)
def watch_folder_sensor(context):
    """
    Sensor que vigila cambios en la carpeta 'data' y en los archivos .py de la raíz.
    """
    # Directorios y archivos a vigilar
    watch_dirs = ["data"]
    watch_patterns = ["*.py"]
    
    files_to_watch = []
    # Añadir archivos en carpetas específicas
    for d in watch_dirs:
        if os.path.exists(d):
            for root, _, files in os.walk(d):
                for f in files:
                    files_to_watch.append(os.path.join(root, f))
    
    # Añadir archivos que coincidan con los patrones en la raíz
    for p in watch_patterns:
        files_to_watch.extend(glob.glob(p))

    # Calcular el tiempo de modificación más reciente
    if not files_to_watch:
        return

    latest_mtime = max(os.path.getmtime(f) for f in files_to_watch)
    curr_mtime_str = str(latest_mtime)
    
    last_mtime_str = context.cursor or "0"
    
    if curr_mtime_str != last_mtime_str:
        # Si hay un cambio, lanzamos el job.
        # Usamos el timestamp como run_key para evitar ejecuciones duplicadas para el mismo estado.
        yield RunRequest(run_key=curr_mtime_str)
        context.update_cursor(curr_mtime_str)

defs = Definitions(
    assets=load_assets_from_modules([lab_renta, interactive_map]),
    asset_checks=load_asset_checks_from_modules([lab_renta_checks]),
    jobs=[all_assets_job],
    sensors=[watch_folder_sensor]
)