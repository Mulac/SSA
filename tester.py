import numpy as np
import pandas as pd


def find_access(frame):
        profiled = frame.loc[ frame['IsOwnedByProfile'] ].copy()
        profileless = frame.loc[ frame['IsOwnedByProfile'] == False ].copy()
        
        profiled['AccessedFrom'] = profiled['Name']
        profileless['AccessedFrom'] = profileless['Name_y']
            
        return pd.concat([profiled, profileless], ignore_index=True) 


# SELECT RELEVANT FIELDS
object_fields = ['SobjectType', 'ParentId', 'PermissionsCreate', 'PermissionsDelete', 'PermissionsEdit',
                 'PermissionsModifyAllRecords', 'PermissionsRead', 'PermissionsViewAllRecords']
field_fields = ['Field', 'ParentId', 'PermissionsEdit', 'PermissionsRead']
assignment_fields = ['AssigneeId', 'PermissionSetId']
permission_set_fields = ['Id', 'Name', 'IsOwnedByProfile', 'ProfileId']
profile_fields = ['Id', 'Name']
user_fields = ['Id', 'Name']
# OPEN SALESFORCE SOURCE FILES
object_permissions = pd.read_csv('ObjectPermissions.csv', usecols=object_fields, false_values=['False'], true_values=['True'])
field_permissions = pd.read_csv('FieldPermissions.csv', usecols=field_fields, false_values=['False'], true_values=['True'])
permission_set_assignment = pd.read_csv('PermissionSetAssignment.csv', usecols=assignment_fields, false_values=['False'], true_values=['True'])
permission_set = pd.read_csv('PermissionSet.csv', false_values=['False'], true_values=['True'])
profile = pd.read_csv('Profile.csv', usecols=profile_fields, false_values=['False'], true_values=['True'])
user = pd.read_csv('User.csv', usecols=user_fields, false_values=['False'], true_values=['True'])
# JOIN TABLES
join1 = pd.merge(permission_set_assignment, user, left_on='AssigneeId', right_on='Id')
join2 = pd.merge(join1, permission_set, left_on='PermissionSetId', right_on='Id')
assignments = pd.merge(join2, profile, left_on='ProfileId', right_on='Id', how='left')
assignments = find_access(assignments) # CALCULATE ORIGINAL ACCESS OF PERMISSION
object_assignments = pd.merge(assignments[['Name_x', 'AccessedFrom', 'PermissionSetId']], object_permissions, left_on='PermissionSetId', right_on='ParentId')
field_assignments = pd.merge(assignments[['Name_x', 'AccessedFrom', 'PermissionSetId']], field_permissions, left_on='PermissionSetId', right_on='ParentId')

# OPEN TEST FILE
tests = pd.read_csv('Tests.csv')
tests.fillna(0, inplace = True)

# CREATE OUTPUT WIREFRAME
result = pd.DataFrame(None,
 columns=['Test', 'User', 'Permission', 'Read', 'Edit', 'Create', 'Delete', 'ViewAll', 'ModifyAll', 'AccessedFrom'])
column_conversions = {
        'Name_x' : 'User',
        'PermissionsRead' : 'Read',
        'PermissionsEdit' : 'Edit',
        'PermissionsCreate' : 'Create',
        'PermissionsDelete' : 'Delete',
        'PermissionsViewAllRecords' : 'ViewAll',
        'PermissionsModifyAllRecords' : 'ModifyAll',
}
types = { 'Object', 'Field', 'System' }


# Takes a query of results and the test no. associated to add to the main results dataframe
def add_result(test, query):
        global result

        query['Test'] = test
        result = result.append(query, ignore_index = True, sort = False)


# Takes a permission and its type, quering the appropriate table and extracts the relavent data
# RETURNS: Dataframe containing all users with perm
def query_tables(permType, perm):
        if permType == 'Object':
                query = object_assignments.loc[ object_assignments['SobjectType'] == perm ].copy()
                query = query[['Name_x', 'PermissionsEdit', 'PermissionsCreate', 'PermissionsRead', 'PermissionsDelete', 'PermissionsViewAllRecords', 'PermissionsModifyAllRecords', 'AccessedFrom']].rename(columns=column_conversions)
        elif permType == 'Field':
                query = field_assignments.loc[ field_assignments['Field'] == perm ].copy()
                query = query[['Name_x', 'PermissionsRead', 'PermissionsEdit', 'AccessedFrom']].rename(columns=column_conversions)
        else:
                query = assignments.loc[ assignments[perm] ].copy()
                query = query[['Name_x', 'AccessedFrom']].rename(columns=column_conversions)

        query['Permission'] = perm
        return query


# TODO: Add type checking (e.g. ensure IF permType1 = System THEN perm1 is a system permission)
for test, series in tests.iterrows():
        # Extract data from each test
        permissionType1, permissionType2 = series['PermissionType1'], series['PermissionType2']
        permission1, permission2 = series['Permission1'], series['Permission2']

        # CASE: SoD
        if (permissionType1 in types) and (permissionType2 in types) and permission1 and permission2: 
                #  -> query both permissions
                query1 = query_tables(permissionType1, permission1)
                query2 = query_tables(permissionType2, permission2)

                #  -> find the common users in query2 from iterating over query1
                common_users = set()
                for index, row in query1.iterrows():
                        if not query2[query2['User'] == row['User']].empty:
                                common_users.add(row['User'])
                #  -> combine the results from both queries where user is common
                for user in common_users:
                        add_result(test, pd.concat([query1[query1['User'] == user], query2[query2['User'] == user]], sort='False'))
        # CASE: Single Duty
        elif (permissionType1 in types) and permission1:
                query = query_tables(permissionType1, permission1)
                add_result(test, query)
        # CASE: Test table not formatted correctly
        else:   
                print('failure: test [%s] could not be interpreted' %test)


result.to_csv('pyOut.csv')
