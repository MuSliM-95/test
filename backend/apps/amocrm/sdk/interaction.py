class BaseInteraction:
    def __init__(self, access_token: str, subdomain: str):
        self.access_token = access_token
        self.subdomain = subdomain

    def get_headers(self):
        headers = {}
        headers.update(self._get_auth_headers())
        return headers

    def _get_auth_headers(self):
        return {"Authorization": "Bearer " + self.access_token}

    def _get_url(self, path):
        return "https://{subdomain}.amocrm.ru/api/v4/{path}".format(subdomain=self.subdomain, path=path)

    def _request(self, method, path, data=None, params=None, headers=None):
        headers = headers or {}
        headers.update(self.get_headers())
        try:
            response = self._session.request(method, url=self._get_url(path), json=data, params=params, headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise exceptions.AmoApiException(e.args[0].args[0])  # Sometimes Connection aborted.
        if response.status_code == 204:
            return None, 204
        if response.status_code < 300 or response.status_code == 400:
            return response.json(), response.status_code
        if response.status_code == 401:
            raise exceptions.UnAuthorizedException()
        if response.status_code == 403:
            raise exceptions.PermissionsDenyException()
        if response.status_code == 402:
            raise ValueError("Тариф не позволяет включать покупателей")
        raise exceptions.AmoApiException("Wrong status {} ({})".format(response.status_code, response.text))

    def request(self, method, path, data=None, params=None, headers=None, include=None):
        params = params or {}
        if include:
            params["with"] = ",".join(include)
        return self._request(method, path, data=data, params=params, headers=headers)

    def _list(self, path, page, include=None, limit=250, query=None, filters: Tuple[Filter] = (), order=None):
        assert order is None or len(order) == 1
        assert limit <= 250
        params = {
            "page": page,
            "limit": limit,
            "query": query,
        }
        if order:
            field, value = list(order.items())[0]
            params["order[{}]".format(field)] = value
        for _filter in filters:
            params.update(_filter._as_params())
        return self.request("get", path, params=params, include=include)

    def _all(self, path, include=None, query=None, filters: Tuple[Filter] = (), order=None, limit=250):
        page = 1
        while True:
            response, _ = self._list(
                path, page, include=include, query=query, filters=filters, order=order, limit=limit
            )
            if response is None:
                return
            yield response["_embedded"]
            if "next" not in response.get("_links", []):
                return
            page += 1