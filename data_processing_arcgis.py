
import arcpy
from arcpy import env
from arcpy.sa import Raster, ZonalStatisticsAsTable

# === Environment ===
env.workspace = r"..\data\MyProject"
env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")

# === === ===
buildings_fc = r"..\data\Building.shp"
ndvi_folder  = r"..\data\NDVI"

district_fc  = r"..\data\Hong_Kong_18_Districts\HKDistrict18.shp"
dcca_fc      = r"..\data\2021PopulationCensusStatisticsByDistrictCouncilConstituencyArea_SHP\DCCA_21C.shp"


# === === ===
years     = list(range(2018, 2025))
threshold = 0.28
area_fld  = "Shape_Are"
zone_fld  = "BUILDINGID"
lyr_name  = "build_lyr"

# === === ===
if arcpy.Exists(lyr_name):
    arcpy.management.Delete(lyr_name)
arcpy.management.MakeFeatureLayer(buildings_fc, lyr_name)

for year in years:
    print(f"\n=== Processing {year} ===")

    ndvi_r = os.path.join(ndvi_folder, f"HK_NDVI_P95_{year}.tif")
    bin_r  = os.path.join(env.workspace, f"debug_bin{year}.tif")
    if arcpy.Exists(bin_r):
        arcpy.management.Delete(bin_r)
    expr = Raster(ndvi_r) > threshold
    expr.save(bin_r)
    print(f">> NDVI > {threshold} → {bin_r}")

    # 2) ZonalStatisticsAsTable
    stat_tbl = os.path.join(env.workspace, f"debug_zonal{year}.dbf")
    if arcpy.Exists(stat_tbl):
        arcpy.management.Delete(stat_tbl)
    print(f">> ZonalStatisticsAsTable → {stat_tbl}")

    ZonalStatisticsAsTable(
        in_zone_data=lyr_name,
        zone_field=zone_fld,
        in_value_raster=bin_r,
        out_table=stat_tbl,
        ignore_nodata="DATA",
        statistics_type=["SUM", "COUNT"]
    )

    # 3)
    sum_dict = {r[0]: r[1] for r in arcpy.da.SearchCursor(stat_tbl, [zone_fld, "SUM"])}
    cnt_dict = {r[0]: r[1] for r in arcpy.da.SearchCursor(stat_tbl, [zone_fld, "COUNT"])}

    # 4)
    ga_field = f"GA{str(year)[-2:]}"
    if ga_field not in [f.name for f in arcpy.ListFields(lyr_name)]:
        arcpy.AddField_management(lyr_name, ga_field, "DOUBLE")

    count_nonzero = 0
    with arcpy.da.UpdateCursor(lyr_name, [zone_fld, area_fld, ga_field]) as cursor:
        for bid, shape_area, old_val in cursor:
            s = sum_dict.get(bid, 0)
            c = cnt_dict.get(bid, 0)
            if c > 0 and shape_area:
                val = (float(s) / float(c)) * float(shape_area)
                count_nonzero += 1
            else:
                val = 0.0
            cursor.updateRow([bid, shape_area, val])

    print(f">> None zoro {ga_field}:", count_nonzero)

print("\n✔ Finish")

# === Spatial Join ===
join = os.path.join(env.workspace, "bld_join_dcca.shp")
final = os.path.join(env.workspace, "Building_Final.shp")

for p in [join1, join2, join3, final]:
    if arcpy.Exists(p):
        arcpy.management.Delete(p)

print("\n=== Spatial Join  ===")

arcpy.analysis.SpatialJoin(lyr_name, dcca_fc, join,
    join_type="KEEP_ALL", match_option="INTERSECT",
    field_mapping=f"*, DCCA_CODE {dcca_fc}.DCCA_CODE")

arcpy.analysis.SpatialJoin(join, district_fc, final,
    join_type="KEEP_ALL", match_option="INTERSECT",
    field_mapping=f"*, DIST_ID {district_fc}.DIST_ID")

print("✔ output:", final)
