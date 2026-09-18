# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Strands agent that helps users draft nx-neptune projections from a data lake.

The agent reads the selected Athena catalog/database schema (metadata only) and
proposes node/edge SQL, then can create a *draft* projection on the user's
behalf. It never executes the import pipeline or touches graph lifecycle — those
remain human-gated actions on the existing validated endpoints.
"""
