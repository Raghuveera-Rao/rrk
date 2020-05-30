#!/usr/bin/env bash
#
# Copyright (c) 2020 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

set -euxo pipefail

# Constants
pravegaWebhookSvc="pravega-webhook-svc"


# Step 1: Create ConfigMap for version_map of bookkeeper, if already created the error is ignored.
cat <<EOF | kubectl create -f - || true
kind: ConfigMap
apiVersion: v1
metadata:
  name: bk-supported-versions-map
data:
  keys: |
        0.1.0:0.1.0
        0.2.0:0.2.0
        0.3.0:0.3.0,0.3.1,0.3.2
        0.3.1:0.3.1,0.3.2
        0.3.2:0.3.2
        0.4.0:0.4.0
        0.5.0:0.5.0,0.5.1,0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.5.1:0.5.1,0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.6.0:0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.6.1:0.6.1,0.6.2,0.7.0,0.7.1
        0.6.2:0.6.2,0.7.0,0.7.1
        0.7.0:0.7.0,0.7.1
        0.7.1:0.7.1
EOF


# Step 2: Create ConfigMap for version_map of pravega, if already created the error is ignored.
cat <<EOF | kubectl create -f - || true
kind: ConfigMap
apiVersion: v1
metadata:
  name: supported-versions-map
data:
  keys: |
        0.1.0:0.1.0
        0.2.0:0.2.0
        0.3.0:0.3.0,0.3.1,0.3.2
        0.3.1:0.3.1,0.3.2
        0.3.2:0.3.2
        0.4.0:0.4.0
        0.5.0:0.5.0,0.5.1,0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.5.1:0.5.1,0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.6.0:0.6.0,0.6.1,0.6.2,0.7.0,0.7.1
        0.6.1:0.6.1,0.6.2,0.7.0,0.7.1
        0.6.2:0.6.2,0.7.0,0.7.1
        0.7.0:0.7.0,0.7.1
        0.7.1:0.7.1
EOF


# Step 3: Create Issuer, if already created the error is ignored.
cat <<EOF | kubectl create -f - || true
apiVersion: cert-manager.io/v1alpha2
kind: Issuer
metadata:
  name: test-selfsigned
  namespace: default
spec:
  selfSigned: {}
EOF

# Step 4: Create Certificate, if already created the error is ignored.
cat <<EOF | kubectl create -f - || true
apiVersion: cert-manager.io/v1alpha2
kind: Certificate
metadata:
  name: selfsigned-cert
  namespace: default
spec:
  secretName: selfsigned-cert-tls
  commonName: $pravegaWebhookSvc.default.svc.cluster.local
  dnsNames:
    - $pravegaWebhookSvc
    - $pravegaWebhookSvc.default.svc.cluster.local
    - $pravegaWebhookSvc.default.svc
  issuerRef:
    name: test-selfsigned
EOF
