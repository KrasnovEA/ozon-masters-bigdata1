from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder, OneHotEncoder
from sklearn.neighbors import KNeighborsClassifier

#
# Dataset fields
#
numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]

fields = ["id", "label"] + numeric_features + categorical_features
#
# Model pipeline
#

# We create the preprocessing pipelines for both numeric and categorical data.

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
])
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('label', OneHotEncoder(handle_unknown='ignore'))
])


preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)

# Now we have a full prediction pipeline.
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('knc', KNeighborsClassifier(n_neighbors=5))
])
