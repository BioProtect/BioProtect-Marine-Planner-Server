from services.file_service import get_output_file


def get_best_solution(obj):
    """Gets the data from the marxan best solution file. These are set on the passed obj in the bestSolution attribute.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    """
    print(f"THE FILE  {obj.output_folder} output_mvbest DOES NOT EXIST")
    print("RESULTS SERVICE file......")
    # filename = get_output_file(obj.output_folder + "output_mvbest")
    # obj.bestSolution = _loadCSV(filename)


def _getOutputSummary(obj):
    """Gets the data from the marxan output summary file. These are set on the passed obj in the outputSummary attribute.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    """
    print(f"THE FILE  {obj.output_folder} output_sum DOES NOT EXIST")
    print("RESULTS SERVICE file......")
    # filename = get_output_file(obj.output_folder + "output_sum")
    # obj.outputSummary = _loadCSV(filename)
