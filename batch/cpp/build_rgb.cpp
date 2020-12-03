#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <sys/stat.h>
#include <math.h>
#include <algorithm>
#include <string.h>
#include <stdint.h>
#include <sstream>
#include <iomanip>
#include <gdal/gdal_priv.h>
#include <gdal/cpl_conv.h>
#include <gdal/ogr_spatialref.h>
using namespace std;

int main(int argc, char* argv[])
{

// Program to take our GLAD UInt16 data (values from 20000 - 39999) and
// convert it into RGB space. The first digit (2/3) is the confidence value,
// followed by the # of days since 2014-12-31. This will convert these
// values to the RGB format used by Vizzuality to decode the tiles on the front end
// More on this format (or how to unpack the RGB values into date + conf + intensity)
// is here: https://github.com/gfw-api/true-color-tiles/blob/master/app/src/services/image.service.js#L14-L46

//passing arguments
if (argc != 4){cout << "Use <program name> <date conf raster> <intensity raster> <output name>" << endl; return 1;}
string in1_name=argv[1];
string in2_name=argv[2];
string out_name=argv[3];

//setting variables
int x, y;
int xsize, ysize;
int total_days;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(in1_name.c_str(), GA_ReadOnly ); INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; uly=GeoTransform[3]; pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;
INGDAL2 = (GDALDataset *) GDALOpen(in2_name.c_str(), GA_ReadOnly ); INBAND2 = INGDAL2->GetRasterBand(1);

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALRasterBand *OUTBAND1;
GDALRasterBand *OUTBAND2;
GDALRasterBand *OUTBAND3;
OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "DEFLATE" );
papszOptions = CSLSetNameValue( papszOptions, "TILED", "YES" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.importFromEPSG(3857);
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };
OUTGDAL = OUTDRIVER->Create( out_name.c_str(), xsize, ysize, 3, GDT_Byte, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ);
OUTBAND1 = OUTGDAL->GetRasterBand(1);
OUTBAND2 = OUTGDAL->GetRasterBand(2);
OUTBAND3 = OUTGDAL->GetRasterBand(3);

//read/write data
uint16_t in1_data[xsize];
uint16_t in2_data[xsize];
uint8_t out_data1[xsize];
uint8_t out_data2[xsize];
uint8_t out_data3[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, in1_data, xsize, 1, GDT_UInt16, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, in2_data, xsize, 1, GDT_UInt16, 0, 0);
for(x=0; x<xsize; x++) {

  if (in1_data[x] < 20000) {
    total_days = 0; }
  else {
    if (in1_data[x] < 30000) {
      total_days = in1_data[x] - 20000; }
  else {
    total_days = in1_data[x] - 30000; }
  }

  out_data1[x] = total_days / 255;

if (in1_data[x] < 20000) {
out_data2[x]=0;
out_data3[x] = in2_data[x];
//cout << unsigned(out_data3[x]) <<", "<<"yep"<< endl;
}
else {
//cout << in1_data[x] <<", "<< unsigned(out_data1[x]) <<", "<< in1_data[x]/255 << endl;
out_data2[x]=stoi(to_string(in1_data[x]).substr(1)) % 255;
if (in1_data[x] >= 20000 && in1_data[x] <= 29999) {
out_data3[x]= 100 + in2_data[x];
} else {
out_data3[x] = 200 + in2_data[x];
}
}
}
OUTBAND1->RasterIO( GF_Write, 0, y, xsize, 1, out_data1, xsize, 1, GDT_Byte, 0, 0 );
OUTBAND2->RasterIO( GF_Write, 0, y, xsize, 1, out_data2, xsize, 1, GDT_Byte, 0, 0 );
OUTBAND3->RasterIO( GF_Write, 0, y, xsize, 1, out_data3, xsize, 1, GDT_Byte, 0, 0 );
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}