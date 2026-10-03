# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - OPTIONAL: MLflow experiment (next-day direction classifier)
# MAGIC Not part of the scheduled job. Run manually to show the ML extension.
# MAGIC This is a learning exercise - daily direction is close to a coin flip, so do not expect high accuracy.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "finance_lakehouse")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

w = Window.partitionBy("ticker").orderBy("trade_date")
data = (
    spark.table(f"{CATALOG}.gold.daily_returns")
    .withColumn("ret_lag1", F.lag("daily_return", 1).over(w))
    .withColumn("ret_lag5", F.lag("daily_return", 5).over(w))
    .withColumn("price_vs_ma20", F.col("close") / F.col("ma_20") - 1)
    .withColumn("price_vs_ma50", F.col("close") / F.col("ma_50") - 1)
    .withColumn("volume_ratio", F.col("volume") / F.col("volume_ma_20"))
    .withColumn("target", (F.lead("daily_return", 1).over(w) > 0).cast("int"))
    .dropna()
)
FEATURES = ["ret_lag1", "ret_lag5", "price_vs_ma20", "price_vs_ma50",
            "volume_ratio", "annualized_volatility", "drawdown"]

pdf = data.select("trade_date", "target", *FEATURES).toPandas().sort_values("trade_date")
split = int(len(pdf) * 0.8)                       # time-based split, no shuffling (avoids look-ahead)
train, test = pdf.iloc[:split], pdf.iloc[split:]

# COMMAND ----------

mlflow.set_experiment(f"/Shared/{CATALOG}_direction_model")
with mlflow.start_run(run_name="random_forest_baseline"):
    params = {"n_estimators": 200, "max_depth": 5, "min_samples_leaf": 50, "random_state": 42}
    model = RandomForestClassifier(**params).fit(train[FEATURES], train["target"])
    proba = model.predict_proba(test[FEATURES])[:, 1]
    mlflow.log_params(params)
    mlflow.log_metric("accuracy", accuracy_score(test["target"], proba > 0.5))
    mlflow.log_metric("roc_auc", roc_auc_score(test["target"], proba))
    mlflow.sklearn.log_model(model, "model")
    print("accuracy:", accuracy_score(test["target"], proba > 0.5), "auc:", roc_auc_score(test["target"], proba))
