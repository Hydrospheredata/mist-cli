def format_request_error(err):
    """
    :param err:
    :rtype str
    :return: message string describing error
    """
    msg = "Error: {}: {}".format(str(err), str(err.response.text))
    request_body = err.request.body
    if request_body is not None:
        msg += '\nRequest body: {}'.format(request_body)
    return msg
