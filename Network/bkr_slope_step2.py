import pandas as pd
import arcpy
import os
import time

# Primary geodatabase for inputs and outputs
geodb = r'V:\TransDeptGIS\GeoDB\Planning\Modeling\BKRCast_BikeNetwork.gdb'
arcpy.env.workspace = geodb

# Emme network shapefile, exported to a geodatabase
in_fc = geodb + r'\baseline_2035_emme_links'

# Elevation raster location
in_raster = r'V:\ExternalData\UWGeology\GeoMapNWFeb2010\usgs_dem_30ft'

# Output dir for final results in csv format
output_dir = r'D:\Bike'

emme_links = arcpy.da.FeatureClassToNumPyArray(in_fc, ('ID','LENGTH','MODES','INODE','JNODE', 'F_biketype'))

print 'loading link files...'
ij_df = pd.read_csv(os.path.join(output_dir, 'ij_df.csv'), sep = ',')
ji_df = pd.read_csv(os.path.join(output_dir, 'ji_df.csv'), sep = ',')

out_point_features = geodb + r'\link_components_elevation'
in_point_features = geodb + r'\link_components_full'


#print 'start to pull elevation'

#if arcpy.Exists(out_point_features):
#    arcpy.Delete_management(out_point_features)

#try:    
#    if arcpy.CheckOutExtension("Spatial") == 'CheckedOut':
#        print 'Spatial Analyst license is checked out'
#        print in_point_features
#        print out_point_features
#        print in_raster
#        arcpy.sa.ExtractValuesToPoints(in_point_features, in_raster, out_point_features)
#    else:
#        print 'Spatial Analyst is required. Tool is terminated'
#except Exception as e:
#    print(e)


print 'load points to numpy...'
# Read resulting intersection of points with elevation into numpy/pandas
# because the MemoryError from FeatureClasstoNumpyArray is frequently popped up, load part of featureclass one time and combine them together later. 
elevation_shp1 = arcpy.da.FeatureClassToNumPyArray(out_point_features, ('RASTERVALU','ID','LENGTH', 'INODE','JNODE'), where_clause = '"RASTERVALU" <= 400')
elevation_shp2 = arcpy.da.FeatureClassToNumPyArray(out_point_features, ('RASTERVALU','ID','LENGTH', 'INODE','JNODE'), where_clause = '"RASTERVALU" > 400')
df1 = pd.DataFrame(elevation_shp1)
df2 = pd.DataFrame(elevation_shp2)

df = pd.concat([df1, df2])
df.set_index('ID', inplace = True)

# List of links IDs
link_list = df.groupby('ID').min().index
print "total links: ", link_list.size
print 'calculating slops'

# Loop through all edges
# Assume that all links are bi-directional and compute ij and ji direction slopes
# if a line is truly one-way, we will discard the ji direction
# since most are two-way it's worth it calculate for all links and merge results later
upslope_ij = {}
upslope_ji = {}
count = 0
link_list_values = link_list.values

for link in link_list_values: 
    link_df = df.loc[[link]]  # use [link] to gurantee a dataframe is returned. 
    if count % 1000 == 0:
        print count
    i_j  = 0
    j_i = 0
    # Extract the elevation data to numPy because it's faster to loop over
    elev_data = link_df['RASTERVALU'].values
    # Loop through each point in each edge
    for point in xrange(len(elev_data)-1):  # stop short of the list because we only want to compare the 2nd to last to last
        elev_diff = elev_data[point+1] - elev_data[point]
        i_j += abs(elev_diff)
        j_i += abs(elev_diff)
        #if elev_diff > 0:
        #    i_j += elev_diff
        #elif elev_diff < 0:
        #    j_i += abs(elev_diff)      # since we know it will be "negative" for the JI direction when calculated
        #                                            # in references to the IJ direction
    upslope_ij[link] = i_j
    upslope_ji[link] = j_i
    count += 1

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

missing_links = df[~df['ID'].isin(emme_attr['ID'].values)]
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
