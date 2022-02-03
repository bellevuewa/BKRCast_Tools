
import pandas as pd
import arcpy
import os

# # Automating Slope Calcs
# - export network shapefile from emme using API
#     - make sure we can set a coordinate system here (projected state plane washington)
# - add shapefile to a geodatabase
# The tool calculates cumulative elevation gains along a link in every 30 feet. the slope
# is then calculated from the cumulative elevation gain. It would describe bicylist's real climbing experience.

# For some unknown reason, this tool would crash when executing ExtractValuesToPoints. Before it is fixed,
# we split the code and make it two scripts. After the crash, run bkr_slope_step2.py to finish calculation.

# ### Define Input Locations
# Primary geodatabase for inputs and outputs
geodb = r'V:\TransDeptGIS\GeoDB\Planning\Modeling\BKRCast_BikeNetwork.gdb'
arcpy.env.workspace = geodb

# Emme network shapefile, exported to a geodatabase
in_fc = geodb + r'\baseline_2035_emme_links'

# Elevation raster location
in_raster = r'V:\ExternalData\UWGeology\GeoMapNWFeb2010\usgs_dem_30ft'

# Output dir for final results in csv format
output_dir = r'D:\Bike'

# ### Load and Process Data
# Find two-way links - we only need to split these links once
emme_links = arcpy.da.FeatureClassToNumPyArray(in_fc, ('ID','LENGTH','MODES','INODE','JNODE', 'F_biketype'))
df = pd.DataFrame(emme_links)

# loop through each link 
ij_links = []
ji_links = []

for rownum in xrange(len(df)):
    inode = df.iloc[rownum].INODE
    jnode = df.iloc[rownum].JNODE
    ij_df = df[(df['INODE']==inode)&(df['JNODE']==jnode)]
    if len(ij_df) == 1:
        ij_id = ij_df.ID.values[0]
    
    ji_df = df[(df['INODE']==jnode)&(df['JNODE']==inode)]
    if len(ji_df) == 1:
        ji_id = ji_df.ID.values[0]
    else:
        # indicates a one-way link with no ji
        # append to ij_links and skip to next
        ij_links.append(ij_id)
        continue
    
    if ji_id not in ij_links:
        ij_links.append(ij_id)
    else:
        ji_links.append(ij_id)

ij_df = df[df['ID'].isin(ij_links)]
ji_df = df[df['ID'].isin(ji_links)]
len(ij_df)+len(ji_df)==len(df)
print 'link df created.'
ij_df.to_csv(os.path.join(output_dir, 'ij_df.csv'), sep = ',')
ji_df.to_csv(os.path.join(output_dir, 'ji_df.csv'), sep = ',')

# - export results to feature class

# - split network lines into points
# - note: this takes 30-60 minutes

# - should only have to do this one time
# - subsequent buffering will compare the imported shapefile links to the result of the cold-start process and only work on the links that haven't been processed yet, appending them to the existing results

# Loop through each network link and split the line into points, saving them in points array
points = []
sr = arcpy.Describe(in_fc).spatialReference
counter = 0

# Set the step based on the line's length
# How many times must the line be split
# 
# Using 30 of fidelity - data is available every 30 meters so this may be unnecessary
segment_len = 30

# Create seperate array that holds tuple of link ID and point coordinates (in state plane coords)
final_result = []
count = 0
with arcpy.da.SearchCursor(in_fc,["SHAPE@",'ID'], spatial_reference=sr) as cursor:  
    for row in cursor:
        # Only process IJ links, because JI are exactly the same polyline shape
        if row[1] in ij_links:
            count += 1
            if count % 1000 == 0:
                print count
            split_count = int(row[0].length/segment_len)
            big_output = []
            for i in range(split_count):
                point = row[0].positionAlongLine(i*segment_len)
                points.append(point)
                #x = point.firstPoint.X
                #y = point.firstPoint.Y
                #point_list = []
                #point_list.append(x)
                #point_list.append(y)
                #output = (str(row[1]), (x, y))
                #final_result.append(output)
    print count
    print 'All lines are split into points.'

# - Export results to a feature class called link_components

point_out_new = r'\link_components'
if arcpy.Exists(point_out_new):
    arcpy.Delete_management(point_out_new)
    
arcpy.CopyFeatures_management(points, geodb + point_out_new) 

# - Intersect the points with the links to get the edge IDs
inFeatures = ["link_components", "links"]
intersectOutput = "link_components_full"
if arcpy.Exists(intersectOutput):
    arcpy.Delete_management(intersectOutput)
clusterTolerance = 1.5    
arcpy.Intersect_analysis(inFeatures, intersectOutput, "", clusterTolerance, "point")

# - Intersect with a raster to get elevation
# - import elevation raster from W:/geodata/raster/dem30m
#     - this is the raster of elevations at 30 m fidelity
#     - note that elevation values are in METERS
# Note that spatial analyst must be active for this portion

# for some unknown reason, the program always get crashed when executing ExtractValuesToPoints.
# license is checked out successfully but it can not be executed. I have to split the script from this point
# into two smaller scripts and they work!

print 'start to pull elevation'
in_point_features = geodb + r'\link_components_full'
out_point_features = geodb + r'\link_components_elevation'
if arcpy.Exists(out_point_features):
    arcpy.Delete_management(out_point_features)

try:    
    if arcpy.CheckOutExtension("Spatial") == 'CheckedOut':
        print 'Spatial Analyst license is checked out'
        print in_point_features
        print out_point_features
        print in_raster
        arcpy.sa.ExtractValuesToPoints(in_point_features, in_raster, out_point_features)
    else:
        print 'Spatial Analyst is required. Tool is terminated'
except Exception as e:
    print(e)


# Read resulting intersection of points with elevation into numpy/pandas
elevation_shp = arcpy.da.FeatureClassToNumPyArray(out_point_features, ('RASTERVALU','ID','LENGTH','INODE','JNODE'))
df = pd.DataFrame(elevation_shp)

# List of links IDs
link_list = df.groupby('ID').min().index

print 'calculating slops'
# Loop through all edges
# Assume that all links are bi-directional and compute ij and ji direction slopes
# if a line is truly one-way, we will discard the ji direction
# since most are two-way it's worth it calculate for all links and merge results later
upslope_ij = {}
upslope_ji = {}
for link in link_list: 
    link_df = df[df['ID'] == link]

    # Extract the elevation data to numPy because it's faster to loop over
    elev_data = link_df['RASTERVALU'].values

    # Loop through each point in each edge
    upslope_ij[link] = 0
    upslope_ji[link] = 0
    for point in xrange(len(elev_data)-1):  # stop short of the list because we only want to compare the 2nd to last to last
        elev_diff = elev_data[point+1] - elev_data[point]
        if elev_diff > 0:
            upslope_ij[link] += elev_diff
        elif elev_diff < 0:
            upslope_ji[link] += abs(elev_diff)      # since we know it will be "negative" for the JI direction when calculated
                                                    # in references to the IJ direction

# Import dictionary to a series and attach upslope back on the original dataframe
upslope_ij_s = pd.Series(upslope_ij, name='elev_gain_ij')
upslope_ji_s = pd.Series(upslope_ji, name='elev_gain_ji')
upslope_ij_s.index.name='ID'
upslope_ij_s = upslope_ij_s.reset_index()
upslope_ji_s.index.name='ID'
upslope_ji_s = upslope_ji_s.reset_index()

print 'processing i-j link'
# Attach ij-direction slope to IJ links
slope_ij = pd.merge(ij_df,upslope_ij_s,on='ID')
slope_ij.rename(columns={"elev_gain_ij": "elev_gain"}, inplace=True)

# Attach ji-direction slope to JI links
print 'processing j-i link'

# fo JI links, flip the i and j values to get lookup of ji links
upslope_ji_s['newID'] = upslope_ji_s.ID.apply(lambda row: row.split('-')[-1]+"-"+row.split('-')[0])
slope_ji = pd.merge(ji_df,upslope_ji_s,left_on='ID',right_on='newID')
slope_ji.rename(columns={"elev_gain_ji": "elev_gain"}, inplace=True)
slope_ji['ID'] = slope_ji['newID']
slope_ji.drop(['ID_x','ID_y','newID'],axis=1,inplace=True)

# Append ji rows to ij to get a complete list of links
slope_df = slope_ij.append(slope_ji)

# Convert elevation into feet from meters
# slope_df['elev_gain'] = slope_df['elev_gain']*3.28084

# Calcualte the average upslope in feet/feet
# Network distance measured in: miles, elevation in meters 
slope_df['avg_upslope'] = slope_df['elev_gain']/(slope_df['LENGTH']*5280)

print 'start exporting'
# - reformat and export as emme_attr.in
# - for BKR, assume all bike facilities are 0 for now

emme_attr = slope_df
print emme_attr.columns
emme_attr.rename(columns={'INODE':'inode','JNODE':'jnode','F_biketype':'@bkfac','avg_upslope':'@upslp'},
                inplace=True)

emme_attr.drop(['LENGTH','elev_gain'], axis=1, inplace=True)

# - some very short links are not processed
# - assume zero elevation change for these

print len(emme_attr)
print len(df)

print emme_attr.columns

# Get list of IDs from network not included in the final outpu
df = pd.DataFrame(emme_links)

missing_links = df[-df['ID'].isin(emme_attr['ID'].values)]
missing_links = missing_links[['ID','INODE','JNODE']]
missing_links.columns = [i.lower() for i in missing_links.columns] 

missing_links['@upslp'] = 0
missing_links['@bkfac'] = 0

emme_attr = emme_attr.append(missing_links)

emme_attr.drop(['id','ID'], axis=1,inplace=True)
emme_attr = emme_attr[['inode','jnode','@bkfac','@upslp']]
emme_attr['@upslp'].fillna(0, inplace = True)

# Export emme transaction file
emme_attr.to_csv(output_dir + r'\emme_attr.in', sep=' ', index=False)

# Export version for use in ArcMap
emme_attr['id']=emme_attr['inode'].astype('str')+'-'+emme_attr['jnode'].astype('str')
emme_attr.to_csv(output_dir + r'\emme_attr.csv', sep=' ', index=False)

emme_attr
print 'done'
