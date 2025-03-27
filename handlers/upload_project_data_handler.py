from handlers.base_handler import BaseHandler
from services.file_service import read_file, get_keys, get_key_value
from services.service_error import raise_error


class UploadInputDatHandler(BaseHandler):
    """REST endpoint to upload a project's input.dat into the database."""

    def initialize(self, pg):
        super().initialize()
        self.pg = pg

    async def post(self):
        try:
            # Validate inputs
            self.validate_args(self.request.arguments, ['project_id'])

            project_id = int(self.get_argument("project_id"))
            input_dat_path = self.folder_project + "input.dat"

            # Read and parse input.dat
            file_content = read_file(input_dat_path)
            keys = get_keys(file_content)

            # Define parameter categories
            input_file_params = ["PUNAME", "SPECNAME",
                                 "PUVSPRNAME", "BOUNDNAME", "BLOCKDEF"]
            run_params = ['BLM', 'PROP', 'RANDSEED', 'NUMREPS', 'NUMITNS', 'STARTTEMP', 'NUMTEMP', 'COSTTHRESH', 'THRESHPEN1', 'THRESHPEN2', 'SAVERUN', 'SAVEBEST', 'SAVESUMMARY',
                          'SAVESCEN', 'SAVETARGMET', 'SAVESUMSOLN', 'SAVEPENALTY', 'SAVELOG', 'RUNMODE', 'MISSLEVEL', 'ITIMPTYPE', 'HEURTYPE', 'CLUMPTYPE', 'VERBOSITY', 'SAVESOLUTIONSMATRIX']
            metadata_params = ['DESCRIPTION', 'CREATEDATE', 'PLANNING_UNIT_NAME',
                               'OLDVERSION', 'IUCN_CATEGORY', 'PRIVATE', 'COSTS']
            renderer_params = ['CLASSIFICATION', 'NUMCLASSES',
                               'COLORCODE', 'TOPCLASSES', 'OPACITY']

            # Parse and insert into respective tables
            for key in keys:
                param, value = get_key_value(file_content, key)

                if key in input_file_params:
                    await self.pg.execute(
                        "INSERT INTO public.project_files (project_id, file_type, file_name) VALUES ($1, $2, $3)",
                        [project_id, param, value]
                    )

                elif key in run_params:
                    await self.pg.execute(
                        "INSERT INTO public.project_run_parameters (project_id, key, value) VALUES ($1, $2, $3)",
                        [project_id, param, str(value)]
                    )

                elif key in renderer_params:
                    await self.pg.execute(
                        "INSERT INTO public.project_renderer (project_id, key, value) VALUES ($1, $2, $3)",
                        [project_id, param, str(value)]
                    )

                elif key in metadata_params:
                    await self.pg.execute(
                        "INSERT INTO public.project_metadata (project_id, key, value) VALUES ($1, $2, $3)",
                        [project_id, param, str(value)]
                    )

                    if param == "PLANNING_UNIT_NAME":
                        # Get planning unit id and update project
                        result = await self.pg.execute("""
                            SELECT unique_id FROM marxan.metadata_planning_units
                            WHERE feature_class_name = $1
                        """, [value], return_format="Dict")
                        if result:
                            planning_unit_id = result[0]["unique_id"]
                            await self.pg.execute(
                                "UPDATE public.projects SET planning_unit_id = $1 WHERE id = $2",
                                [planning_unit_id, project_id]
                            )

            self.send_response(
                {"info": "input.dat successfully uploaded to database."})

        except Exception as e:
            raise_error(self, str(e))
