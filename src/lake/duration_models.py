"""Comparaison de modeles ML pour la prediction de duree de trajet."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer


def _build_feature_matrix(pdf: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    cols = ["pickup_hour", "pickup_dow", "pu_location_id", "trip_distance", "vehicle_type"]
    available = [c for c in cols if c in pdf.columns]
    if "vehicle_type" not in available:
        available = [c for c in cols[:-1] if c in pdf.columns]

    features = pdf[available].copy()
    for col in ("pickup_hour", "pickup_dow", "pu_location_id", "trip_distance"):
        if col in features.columns:
            features[col] = features[col].fillna(0)
    if "vehicle_type" in features.columns:
        features["vehicle_type"] = features["vehicle_type"].fillna("unknown")

    target = pdf["trip_duration_sec"]
    return features, target, available


def _get_models() -> dict[str, Any]:
    return {
        "LinearRegression": LinearRegression(),
        "Ridge": Ridge(alpha=1.0),
        "RandomForest": RandomForestRegressor(
            n_estimators=80,
            max_depth=12,
            min_samples_leaf=5,
            n_jobs=-1,
            random_state=42,
        ),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=80,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
        ),
    }


def _fit_with_encoding(
    model: Any,
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    categorical: list[str],
) -> Any:
    if not categorical:
        fitted = model.fit(x_train, y_train)
        preds = fitted.predict(x_test)
        return fitted, preds

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical),
            ("num", "passthrough", [c for c in x_train.columns if c not in categorical]),
        ]
    )
    pipeline = Pipeline([("prep", preprocessor), ("model", model)])
    pipeline.fit(x_train, y_train)
    return pipeline, pipeline.predict(x_test)


def train_gradient_boosting(pdf: pd.DataFrame, test_size: float = 0.2) -> dict[str, Any]:
    """Entraine GradientBoosting (modele retenu en production)."""
    features, target, feature_names = _build_feature_matrix(pdf)
    categorical = [c for c in ("vehicle_type",) if c in feature_names]

    x_train, x_test, y_train, y_test = train_test_split(
        features, target, test_size=test_size, random_state=42
    )

    model = GradientBoostingRegressor(
        n_estimators=80,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
    )
    _, preds = _fit_with_encoding(model, x_train, x_test, y_train, categorical)
    mae = float(mean_absolute_error(y_test, preds))
    r2 = float(r2_score(y_test, preds))

    return {
        "target": "trip_duration_sec",
        "sample_rows": len(pdf),
        "features": feature_names,
        "model": "GradientBoosting",
        "mae_seconds": round(mae, 2),
        "r2_score": round(r2, 4),
        "train_rows": len(x_train),
        "test_rows": len(x_test),
    }


def compare_duration_models(pdf: pd.DataFrame, test_size: float = 0.2) -> dict[str, Any]:
    features, target, feature_names = _build_feature_matrix(pdf)
    categorical = [c for c in ("vehicle_type",) if c in feature_names]

    x_train, x_test, y_train, y_test = train_test_split(
        features, target, test_size=test_size, random_state=42
    )

    results: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None

    for name, model in _get_models().items():
        fitted, preds = _fit_with_encoding(model, x_train, x_test, y_train, categorical)
        mae = float(mean_absolute_error(y_test, preds))
        r2 = float(r2_score(y_test, preds))
        entry: dict[str, Any] = {
            "model": name,
            "mae_seconds": round(mae, 2),
            "r2_score": round(r2, 4),
            "train_rows": len(x_train),
            "test_rows": len(x_test),
        }

        if name == "LinearRegression" and not categorical:
            entry["coefficients"] = dict(
                zip(feature_names, [round(float(c), 4) for c in fitted.coef_])
            )

        results.append(entry)
        if best is None or mae < best["mae_seconds"] or (mae == best["mae_seconds"] and r2 > best["r2_score"]):
            best = {**entry, "features": feature_names}

    results.sort(key=lambda r: (r["mae_seconds"], -r["r2_score"]))

    return {
        "target": "trip_duration_sec",
        "sample_rows": len(pdf),
        "features": feature_names,
        "comparison": results,
        "best_model": best,
        # Compat ancien format MongoDB / metrics
        "model": best["model"] if best else None,
        "mae_seconds": best["mae_seconds"] if best else None,
        "r2_score": best["r2_score"] if best else None,
        "train_rows": best["train_rows"] if best else 0,
        "test_rows": best["test_rows"] if best else 0,
    }
