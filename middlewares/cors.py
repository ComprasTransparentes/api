""" A CORS middleware for the Falcon Framework <http://falconframework.org/>
"""
__author__ = "Luis Benitez"
__license__ = "MIT"

from falcon import HTTP_METHODS


class CorsMiddleware(object):
    """Implements (partially) the Cross Origin Resource Sharing specification
    Link: http://www.w3.org/TR/cors/
    """

    ALLOWED_ORIGINS = ['*']

    def process_resource(self, req, resp, resource):
        origin = req.get_header('Origin')
        if origin:
            # If there is no Origin header, then it is not a valid CORS request
            acrm = req.get_header('Access-Control-Request-Method')
            acrh = req.get_header('Access-Control-Request-Headers')
            if req.method == 'OPTIONS' and acrm and acrh:
                # Method is OPTIONS & ACRM & ACRH Headers => This is a preflight request
                # TODO Validate ACRM & ACRH

                # Set ACAH to echo ACRH
                resp.set_header('Access-Control-Allow-Headers', acrh)

                # Optionally set ACMA
                # resp.set_header('Access-Control-Max-Age', '60')

                # Find implemented methods
                allowed_methods = []
                for method in HTTP_METHODS:
                    allowed_method = getattr(resource, 'on_' + method.lower(), None)
                    if allowed_method:
                        allowed_methods.append(method)
                # Fill ACAM
                resp.set_header('Access-Control-Allow-Methods', ','.join(sorted(allowed_methods)))

    def process_response(self, req, resp, resource):
        origin = req.get_header('Origin')
        if origin:
            # If there is no Origin header, then it is not a valid CORS request
            if '*' in self.ALLOWED_ORIGINS:
                resp.set_header('Access-Control-Allow-Origin', '*')
            elif origin in self.ALLOWED_ORIGINS:
                resp.set_header('Access-Control-Allow-Origin', origin)
