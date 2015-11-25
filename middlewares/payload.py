import falcon
import json


class PayloadParserMiddleware(object):
    """Parses the request body into Request.context['payload'] based on Request.content_type"""

    def process_request(self, req, resp):
        """Process request"""

        # Do nothing if there is no content_type
        if req.content_type is None:
            return

        # Check if application/json is in content_type
        if 'application/json' in req.content_type and req.method not in ['HEAD', 'GET']:
            try:
                data = json.load(req.stream)
            except ValueError:
                title = "Wrong request payload format"
                description = "Request's content-type is %s, but request's payload is not." % req.content_type
                raise falcon.HTTPBadRequest(title, description)

            req.context['payload'] = data
        else:
            pass
