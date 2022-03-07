package io.managed.services.test.client.securitymgmt;

import com.openshift.cloud.api.kas.SecurityApi;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.ApiException;
import com.openshift.cloud.api.kas.invoker.Pair;
import com.openshift.cloud.api.kas.models.Error;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountList;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.client.securitymgmt.models.TemporaryError;
import io.managed.services.test.client.securitymgmt.models.TemporaryServiceAccount;
import io.managed.services.test.client.securitymgmt.models.TemporaryServiceAccountList;
import io.managed.services.test.client.securitymgmt.models.TemporaryServiceAccountRequest;

import javax.ws.rs.core.GenericType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This Class is a clone of the SecurityApi of the kafka-management-sdk (v0.15.5) package with the only difference
 * being that the api path doesn't have the hardcoded /api/kafkas_mgmt/v1 prefix so that it can be used to target
 * the security api on
 */
public class TemporarySecurityApi extends SecurityApi {

    public TemporarySecurityApi(ApiClient apiClient) {
        super(apiClient);
    }

    @Override
    public ServiceAccount createServiceAccount(ServiceAccountRequest serviceAccountRequest) throws ApiException {
        var apiClient = getApiClient();

        // verify the required parameter 'serviceAccountRequest' is set
        if (serviceAccountRequest == null) {
            throw new ApiException(400, "Missing the required parameter 'serviceAccountRequest' when calling createServiceAccount");
        }

        var localVarPostBody = TemporaryServiceAccountRequest.fromServiceAccountRequest(serviceAccountRequest);

        // create path and map variables
        String localVarPath = "";

        // query params
        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();


        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

        final String[] localVarContentTypes = {
            "application/json"
        };
        final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

        String[] localVarAuthNames = new String[] {"Bearer"};

        GenericType<TemporaryServiceAccount> localVarReturnType = new GenericType<>() {
        };
        return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType)
            .toServiceAccount();
    }

    @Override
    public Error deleteServiceAccountById(String id) throws ApiException {
        var apiClient = getApiClient();
        Object localVarPostBody = null;

        // verify the required parameter 'id' is set
        if (id == null) {
            throw new ApiException(400, "Missing the required parameter 'id' when calling deleteServiceAccountById");
        }

        // create path and map variables
        String localVarPath = "/{id}".replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id));

        // query params
        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();


        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

        final String[] localVarContentTypes = {

        };
        final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

        String[] localVarAuthNames = new String[] {"Bearer"};

        GenericType<TemporaryError> localVarReturnType = new GenericType<>() {
        };
        var error = apiClient.invokeAPI(localVarPath, "DELETE", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType);
        if (error != null) {
            return error.toError();
        }
        return new Error();
    }

    @Override
    public ServiceAccount getServiceAccountById(String id) throws ApiException {
        var apiClient = getApiClient();
        Object localVarPostBody = null;

        // verify the required parameter 'id' is set
        if (id == null) {
            throw new ApiException(400, "Missing the required parameter 'id' when calling getServiceAccountById");
        }

        // create path and map variables
        String localVarPath = "/{id}".replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id.toString()));

        // query params
        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();


        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

        final String[] localVarContentTypes = {

        };
        final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

        String[] localVarAuthNames = new String[] {"Bearer"};

        GenericType<TemporaryServiceAccount> localVarReturnType = new GenericType<>() {
        };
        return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType)
            .toServiceAccount();
    }

    @Override
    public ServiceAccountList getServiceAccounts() throws ApiException {
        var apiClient = getApiClient();
        Object localVarPostBody = null;

        // create path and map variables
        String localVarPath = "";

        // query params
        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();


        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

        final String[] localVarContentTypes = {

        };
        final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

        String[] localVarAuthNames = new String[] {"Bearer"};

        GenericType<TemporaryServiceAccountList> localVarReturnType = new GenericType<>() {
        };
        return apiClient.invokeAPI(localVarPath, "GET", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType)
            .toServiceAccountList();
    }

    @Override
    public ServiceAccount resetServiceAccountCreds(String id) throws ApiException {
        var apiClient = getApiClient();
        Object localVarPostBody = null;

        // verify the required parameter 'id' is set
        if (id == null) {
            throw new ApiException(400, "Missing the required parameter 'id' when calling resetServiceAccountCreds");
        }

        // create path and map variables
        String localVarPath = "/{id}/resetSecret".replaceAll("\\{" + "id" + "\\}", apiClient.escapeString(id));

        // query params
        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();


        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

        final String[] localVarContentTypes = {

        };
        final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

        String[] localVarAuthNames = new String[] {"Bearer"};

        GenericType<TemporaryServiceAccount> localVarReturnType = new GenericType<>() {
        };
        return apiClient.invokeAPI(localVarPath, "POST", localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAccept, localVarContentType, localVarAuthNames, localVarReturnType)
            .toServiceAccount();
    }
}
