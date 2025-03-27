class CumulativeImpactHandler(MarxanWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Running Cumulative Impact Function..."})
        except MarxanServicesError as e:  # authentication/authorisation error
            print('MarxanServicesError as e: ', e)
            pass
        else:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['selectedIds'])

            id = None
            nodata_val = 0
            connect_str = psql_str()
            activity = self.get_argument('activity')
            feature_class_name = _getUniqueFeatureclassName("impact_")
            self.send_response({'info': "Building sensitivity matrix..."})
            stressors_list = get_tif_list('/data/pressures', 'tif')
            ecosys_list = get_tif_list('/data/rasters/ecosystem', 'tif')
            sens_mat = setup_sens_matrix()

            self.send_response(
                {'info': "Running Cumulative Impact Function..."})
            impact, meta = cumul_impact(ecosys_list,
                                        sens_mat,
                                        stressors_list,
                                        nodata_val)
            # set all zeroes to nodata val
            # impact = np.where(impact == 0, nodata_val, impact)

            self.send_response({'info': "Reprojecting rasters..."})
            reproject_raster_to_all_habs(tmp_file='./data/tmp/impact2.tif',
                                         data=impact,
                                         meta=meta,
                                         out_file='./data/tmp/marxan.tif')
            impact_file = 'data/uploaded_rasters/impact.tif'
            cropped_impact = 'data/uploaded_rasters/'+feature_class_name+'.tif'
            project_raster(rast1='data/tmp/impact.tif',
                           template_file='data/rasters/all_habitats.tif',
                           output_file=impact_file)

            wgs84_rast = reproject_raster(file=impact_file,
                                          output_folder='data/tmp/',
                                          reprojection_crs=config["wgs84_str"])

            try:
                self.send_response(
                    {'info': 'Saving cumulative impact raster to database...'})
                cmds = "raster2pgsql -s 4326 -c -I -C -F " + wgs84_rast + \
                    " marxan." + feature_class_name + connect_str
                subprocess.call(cmds, shell=True)
            except TypeError as e:
                self.send_response(
                    {'error': 'Unable to save Cumulative Impact raster to database...'})
                print(
                    "Pass in the location of the file as a string, not anything else....")

            try:
                self.send_response(
                    {'info': 'Saving to meta data table and uploading to mapbox...'})
                id = await _finishImportingImpact(feature_class_name,
                                                  activity.replace(
                                                      ' ', '_').lower(),
                                                  self.get_argument(
                                                      'description'),
                                                  self.get_current_user())
                # start the upload to mapbox
                # uploadId = uploadRasterToMapbox(wgs84_rast, feature_class_name)
                #######################################################################
                # chill on uploading to mapbox for ther minunte
                print('uploadId: ', uploadId)
                self.send_response({'info': "Raster '" + activity + "' created",
                                    'feature_class_name': feature_class_name,
                                    'uploadId': uploadId})

                self.close({
                    'info': "Cumulative Impact run and raster uploaded to mapbox",
                    'uploadId': uploadId
                })

            except (MarxanServicesError) as e:
                print('e: ', e)
                self.close({
                    'error': e.args[0],
                    'info': 'Failed to run CI function....'
                })
