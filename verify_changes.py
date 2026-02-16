
import pandas as pd
import plotnine as p9
import os
from lab_renta import renta_cleaning, renta_visualizations, renta_load

def test_renta_cleaning():
    print("Testing renta_cleaning...")
    # Load raw data
    raw_data = renta_load()
    
    # Run cleaning
    cleaned_data = renta_cleaning(raw_data)
    
    # Check columns
    expected_columns = ['region', 'year', 'measure', 'value']
    assert list(cleaned_data.columns) == expected_columns, f"Expected columns {expected_columns}, but got {list(cleaned_data.columns)}"
    
    # Check if empty columns are gone
    forbidden_columns = ["ESTADO_OBSERVACION#es", "CONFIDENCIALIDAD_OBSERVACION#es", "TERRITORIO_CODE", "TIME_PERIOD_CODE", "MEDIDAS_CODE"]
    for col in forbidden_columns:
        assert col not in cleaned_data.columns, f"Column {col} should have been dropped"
        
    # Check for nulls in key columns
    assert cleaned_data['region'].isnull().sum() == 0, "Nulls found in region"
    assert cleaned_data['year'].isnull().sum() == 0, "Nulls found in year"
    assert cleaned_data['measure'].isnull().sum() == 0, "Nulls found in measure"
    assert cleaned_data['value'].isnull().sum() == 0, "Nulls found in value"
    
    print("renta_cleaning passed!")
    return cleaned_data

def test_renta_visualizations(cleaned_data):
    print("Testing renta_visualizations...")
    # Ensure previous plot is removed if exists
    if os.path.exists("renta_plot_region.png"):
        os.remove("renta_plot_region.png")
        
    # Run visualization
    plot = renta_visualizations(cleaned_data)
    
    # Check if file serves
    # plot.save() is called inside the function
    assert os.path.exists("renta_plot_region.png"), "Plot file was not created"
    
    print("renta_visualizations passed!")

if __name__ == "__main__":
    try:
        cleaned_data = test_renta_cleaning()
        test_renta_visualizations(cleaned_data)
        print("All tests passed!")
    except Exception as e:
        print(f"Test failed: {e}")
        exit(1)
