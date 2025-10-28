/**
 * System Admin API Integration Test
 *
 * PURPOSE:
 * Validates all System Management Admin APIs for managing system configurations
 * that define how Knowledge Bases are created (AI model family, embedding
 * dimensionality, RAG settings).
 *
 * TEST FLOW:
 * Phase 1: ListSystemsAdmin - Verify default systems exist
 * Phase 2: GetSystemAdmin - Retrieve specific system configs
 * Phase 3: GetDefaultSystemAdmin - Verify default system
 * Phase 4: CreateSystemAdmin - Create custom system
 * Phase 5: UpdateSystemAdmin - Update with field mask
 * Phase 6: RenameSystemAdmin - Change system ID
 * Phase 7: SetDefaultSystemAdmin - Change default system
 * Phase 8: DeleteSystemAdmin - Remove custom system
 * Phase 9: Verify protected systems cannot be deleted/renamed
 *
 * KEY VALIDATIONS:
 * - All CRUD operations work correctly
 * - Field mask properly filters updates
 * - Rename operation changes system ID
 * - Default system management works
 * - Protected "openai" system cannot be deleted/renamed
 * - System config follows RFC-1034 for IDs
 * - System resource name format: systems/{system_id}
 */

import grpc from "k6/net/grpc";
import { check, group } from "k6";
import http from "k6/http";

import * as constant from "./const.js";

const client = new grpc.Client();
client.load(
    ["./proto"],
    "artifact/artifact/v1alpha/artifact_private_service.proto"
);

export let options = {
    setupTimeout: '300s',
    teardownTimeout: '60s',
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

export function setup() {
    check(true, { [constant.banner('System Admin API Test: Setup')]: () => true });

    // Generate unique test prefix
    const dbIDPrefix = constant.generateDBIDPrefix();
    console.log(`grpc-system-admin.js: Using unique test prefix: ${dbIDPrefix}`);

    // Authenticate
    const loginResp = http.request("POST", `${constant.mgmtRESTPublicHost}/v1beta/auth/login`, JSON.stringify({
        "username": constant.defaultUsername,
        "password": constant.defaultPassword,
    }));

    check(loginResp, {
        "Setup: Authentication successful": (r) => r.status === 200,
    });

    const grpcMetadata = {
        "metadata": {
            "Authorization": `Bearer ${loginResp.json().accessToken}`,
        },
        "timeout": "300s",
    };

    return {
        metadata: grpcMetadata,
        dbIDPrefix: dbIDPrefix,
    };
}

export function teardown(data) {
    // Cleanup any test systems that might remain
    group("System Admin Test: Teardown", () => {
        check(true, { [constant.banner('Teardown')]: () => true });

        // Clean up systems created during the test
        const testSystemIds = [];
        if (data.customSystemId) testSystemIds.push(data.customSystemId);
        if (data.renamedSystemId) testSystemIds.push(data.renamedSystemId);

        for (const systemId of testSystemIds) {
            try {
                client.invoke(
                    "artifact.artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                    { id: systemId },
                    data.metadata
                );
                console.log(`Teardown: Deleted system ${systemId}`);
            } catch (e) {
                // Ignore errors during cleanup (system might already be deleted)
            }
        }

        client.close();
    });
}

export default function (data) {
    // Connect gRPC client to private service
    client.connect(constant.artifactGRPCPrivateHost, {
        plaintext: true,
    });

    group("System Admin API: Complete Test Suite", () => {

        // ====================================================================
        // PHASE 1: List Systems
        // ====================================================================
        group("Phase 1: List all system configurations", () => {
            console.log("\n=== Phase 1: Listing all systems ===");

            const listRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/ListSystemsAdmin",
                {},
                data.metadata
            );

            check(listRes, {
                "Phase 1: List systems successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 1: Has systems array": (r) => r.message && Array.isArray(r.message.systems),
                "Phase 1: Has at least 2 systems (openai, gemini)": (r) => r.message.systems.length >= 2,
            });

            if (listRes.status === grpc.StatusOK) {
                console.log(`Phase 1: Found ${listRes.message.systems.length} systems`);
                listRes.message.systems.forEach((system, idx) => {
                    console.log(`  ${idx + 1}. System ID: ${system.id}, Default: ${system.isDefault || false}`);
                });
            }
        });

        // ====================================================================
        // PHASE 2: Get Specific Systems
        // ====================================================================
        group("Phase 2: Get specific system configurations", () => {
            console.log("\n=== Phase 2: Getting specific systems ===");

            // Get OpenAI system
            const openaiRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: "openai" },
                data.metadata
            );

            check(openaiRes, {
                "Phase 2: Get OpenAI system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 2: OpenAI system has correct ID": (r) => r.message.system.id === "openai",
                "Phase 2: OpenAI system has UID": (r) => r.message.system.uid && r.message.system.uid.length > 0,
                "Phase 2: OpenAI system has name": (r) => r.message.system.name === "systems/openai",
                "Phase 2: OpenAI system has config": (r) => r.message.system.config !== null,
                "Phase 2: OpenAI config has model_family": (r) =>
                    r.message.system.config.rag.embedding.model_family === "openai",
                "Phase 2: OpenAI config has dimensionality": (r) =>
                    r.message.system.config.rag.embedding.dimensionality === 1536,
            });

            // Get Gemini system
            const geminiRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: "gemini" },
                data.metadata
            );

            check(geminiRes, {
                "Phase 2: Get Gemini system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 2: Gemini system has correct ID": (r) => r.message.system.id === "gemini",
                "Phase 2: Gemini system has name": (r) => r.message.system.name === "systems/gemini",
                "Phase 2: Gemini config has model_family": (r) =>
                    r.message.system.config.rag.embedding.model_family === "gemini",
                "Phase 2: Gemini config has dimensionality": (r) =>
                    r.message.system.config.rag.embedding.dimensionality === 3072,
            });
        });

        // ====================================================================
        // PHASE 3: Get Default System
        // ====================================================================
        group("Phase 3: Get default system configuration", () => {
            console.log("\n=== Phase 3: Getting default system ===");

            const defaultRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            check(defaultRes, {
                "Phase 3: Get default system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 3: Default system has ID": (r) => r.message.system.id && r.message.system.id.length > 0,
                "Phase 3: Default system is_default is true": (r) => r.message.system.isDefault === true,
            });

            if (defaultRes.status === grpc.StatusOK) {
                console.log(`Phase 3: Default system is: ${defaultRes.message.system.id}`);
            }
        });

        // ====================================================================
        // PHASE 4: Create Custom System
        // ====================================================================
        let customSystemUid = null;
        let customSystemId = null;
        group("Phase 4: Create custom system configuration", () => {
            console.log("\n=== Phase 4: Creating custom system ===");

            // Use unique system ID to avoid conflicts with previous test runs
            customSystemId = `${data.dbIDPrefix}sysadmin-custom`;

            const createRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/CreateSystemAdmin",
                {
                    system: {
                        id: customSystemId,
                        config: {
                            rag: {
                                embedding: {
                                    model_family: "openai",
                                    dimensionality: 1536
                                }
                            }
                        },
                        description: "Test custom system for integration testing"
                    }
                },
                data.metadata
            );

            check(createRes, {
                "Phase 4: Create system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 4: Created system has correct ID": (r) => r && r.message && r.message.system && r.message.system.id === customSystemId,
                "Phase 4: Created system has UID": (r) => r && r.message && r.message.system && r.message.system.uid && r.message.system.uid.length > 0,
                "Phase 4: Created system has resource name": (r) => r && r.message && r.message.system && r.message.system.name === `systems/${customSystemId}`,
                "Phase 4: Created system is not default": (r) => r && r.message && r.message.system && r.message.system.isDefault === false,
                "Phase 4: Created system has timestamps": (r) => r && r.message && r.message.system && r.message.system.createTime && r.message.system.updateTime,
            });

            if (createRes.status === grpc.StatusOK) {
                customSystemUid = createRes.message.system.uid;
                data.customSystemId = customSystemId; // Store for teardown
                console.log(`Phase 4: Created system with ID: ${customSystemId}, UID: ${customSystemUid}`);
            } else {
                console.error(`Phase 4: Create system FAILED - Status: ${createRes.status}, Error: ${createRes.error ? createRes.error.message : 'unknown'}`);
            }
        });

        // ====================================================================
        // PHASE 5: Update System with Field Mask
        // ====================================================================
        group("Phase 5: Update system with field mask", () => {
            console.log("\n=== Phase 5: Updating system description only (field mask) ===");

            if (!customSystemId) {
                console.error("Phase 5: Skipping - custom system ID not available from Phase 4");
                return;
            }

            console.log(`Phase 5: Using customSystemId: ${customSystemId} (type: ${typeof customSystemId})`);

            const updateRequest = {
                system: {
                    id: customSystemId,
                    description: "Updated description via field mask"
                },
                update_mask: "description"  // k6 gRPC uses snake_case string for field mask
            };
            console.log(`Phase 5: Request object: ${JSON.stringify(updateRequest)}`);

            const updateRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/UpdateSystemAdmin",
                updateRequest,
                data.metadata
            );

            if (updateRes.status !== grpc.StatusOK) {
                console.error(`Phase 5: Update FAILED - Status: ${updateRes.status}, Error: ${updateRes.error ? updateRes.error.message : 'unknown'}`);
            }

            check(updateRes, {
                "Phase 5: Update system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 5: Description was updated": (r) =>
                    r && r.message && r.message.system && r.message.system.description === "Updated description via field mask",
                "Phase 5: Config remains unchanged": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.model_family === "openai" &&
                    r.message.system.config.rag.embedding.dimensionality === 1536,
                "Phase 5: UID unchanged": (r) => r && r.message && r.message.system && r.message.system.uid === customSystemUid,
            });

            // Update both config and description
            console.log("\n=== Phase 5: Updating config with field mask ===");

            const updateConfigRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/UpdateSystemAdmin",
                {
                    system: {
                        id: customSystemId,
                        config: {
                            rag: {
                                embedding: {
                                    model_family: "gemini",
                                    dimensionality: 3072
                                }
                            }
                        },
                        description: "Updated config to Gemini"
                    },
                    update_mask: "config,description"  // Comma-separated string for multiple fields
                },
                data.metadata
            );

            if (updateConfigRes.status !== grpc.StatusOK) {
                console.error(`Phase 5: Config update FAILED - Status: ${updateConfigRes.status}, Error: ${updateConfigRes.error ? updateConfigRes.error.message : 'unknown'}`);
            }

            check(updateConfigRes, {
                "Phase 5: Update config successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 5: Config was updated to Gemini": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.model_family === "gemini",
                "Phase 5: Dimensionality was updated": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.dimensionality === 3072,
                "Phase 5: Description was also updated": (r) =>
                    r && r.message && r.message.system && r.message.system.description === "Updated config to Gemini",
            });
        });

        // ====================================================================
        // PHASE 6: Rename System
        // ====================================================================
        group("Phase 6: Rename system configuration", () => {
            console.log("\n=== Phase 6: Renaming system ID ===");

            if (!customSystemId) {
                console.error("Phase 6: Skipping - custom system ID not available from Phase 4");
                return;
            }

            const newSystemId = `${data.dbIDPrefix}sysadmin-renamed`;

            const renameRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/RenameSystemAdmin",
                {
                    systemId: customSystemId,
                    newSystemId: newSystemId
                },
                data.metadata
            );

            check(renameRes, {
                "Phase 6: Rename system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 6: System ID was changed": (r) => r.message.system.id === newSystemId,
                "Phase 6: Resource name was updated": (r) => r.message.system.name === `systems/${newSystemId}`,
                "Phase 6: UID remains unchanged": (r) => r.message.system.uid === customSystemUid,
                "Phase 6: Config preserved": (r) =>
                    r.message.system.config.rag.embedding.model_family === "gemini",
            });

            if (renameRes.status === grpc.StatusOK) {
                data.renamedSystemId = newSystemId; // Store for teardown
                data.customSystemId = null; // Old ID no longer valid
            }

            // Verify old ID no longer exists
            const getOldRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: customSystemId },
                data.metadata
            );

            check(getOldRes, {
                "Phase 6: Old ID no longer accessible": (r) => r && r.status !== grpc.StatusOK,
            });

            // Verify new ID is accessible
            const getNewRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: newSystemId },
                data.metadata
            );

            check(getNewRes, {
                "Phase 6: New ID is accessible": (r) => r && r.status === grpc.StatusOK,
                "Phase 6: Retrieved system has new ID": (r) => r.message.system.id === newSystemId,
            });
        });

        // ====================================================================
        // PHASE 7: Set Default System
        // ====================================================================
        group("Phase 7: Change default system", () => {
            console.log("\n=== Phase 7: Setting custom system as default ===");

            const renamedSystemId = data.renamedSystemId;
            if (!renamedSystemId) {
                console.error("Phase 7: Skipping - renamed system ID not available from Phase 6");
                return;
            }

            // First, get current default
            const getCurrentDefaultRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            let previousDefaultId = null;
            if (getCurrentDefaultRes.status === grpc.StatusOK) {
                previousDefaultId = getCurrentDefaultRes.message.system.id;
                console.log(`Phase 7: Current default is: ${previousDefaultId}`);
            }

            // Set custom system as default
            const setDefaultRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/SetDefaultSystemAdmin",
                { id: renamedSystemId },
                data.metadata
            );

            check(setDefaultRes, {
                "Phase 7: Set default successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 7: System is now default": (r) => r.message.system.isDefault === true,
                "Phase 7: Correct system was set": (r) => r.message.system.id === renamedSystemId,
            });

            // Verify via GetDefaultSystemAdmin
            const verifyDefaultRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            check(verifyDefaultRes, {
                "Phase 7: GetDefault returns new default": (r) =>
                    r && r.status === grpc.StatusOK &&
                    r.message.system.id === renamedSystemId,
            });

            // Restore original default
            if (previousDefaultId && previousDefaultId !== renamedSystemId) {
                console.log(`Phase 7: Restoring original default: ${previousDefaultId}`);
                const restoreRes = client.invoke(
                    "artifact.artifact.v1alpha.ArtifactPrivateService/SetDefaultSystemAdmin",
                    { id: previousDefaultId },
                    data.metadata
                );

                check(restoreRes, {
                    "Phase 7: Restore original default successful": (r) => r && r.status === grpc.StatusOK,
                });
            }
        });

        // ====================================================================
        // PHASE 8: Delete Custom System
        // ====================================================================
        group("Phase 8: Delete custom system", () => {
            console.log("\n=== Phase 8: Deleting custom system ===");

            const renamedSystemId = data.renamedSystemId;
            if (!renamedSystemId) {
                console.error("Phase 8: Skipping - renamed system ID not available");
                return;
            }

            const deleteRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                { id: renamedSystemId },
                data.metadata
            );

            check(deleteRes, {
                "Phase 8: Delete system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 8: Delete returned success": (r) => r.message.success === true,
            });

            // Verify system is deleted
            const getDeletedRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: renamedSystemId },
                data.metadata
            );

            check(getDeletedRes, {
                "Phase 8: Deleted system not accessible": (r) => r && r.status !== grpc.StatusOK,
            });

            // Mark as deleted for teardown
            if (deleteRes.status === grpc.StatusOK) {
                data.renamedSystemId = null;
            }
        });

        // ====================================================================
        // PHASE 9: Verify Protected Systems
        // ====================================================================
        group("Phase 9: Verify protected system cannot be deleted or renamed", () => {
            console.log("\n=== Phase 9: Testing protected 'openai' system ===");

            // Try to delete openai system (should fail)
            const deleteOpenaiRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                { id: "openai" },
                data.metadata
            );

            check(deleteOpenaiRes, {
                "Phase 9: Cannot delete openai system": (r) => r && r.status !== grpc.StatusOK,
            });

            // Try to rename openai system (should fail)
            const renameOpenaiRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/RenameSystemAdmin",
                {
                    systemId: "openai",
                    newSystemId: "openai-renamed"
                },
                data.metadata
            );

            check(renameOpenaiRes, {
                "Phase 9: Cannot rename openai system": (r) => r && r.status !== grpc.StatusOK,
            });

            // Verify openai system still exists
            const getOpenaiRes = client.invoke(
                "artifact.artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { id: "openai" },
                data.metadata
            );

            check(getOpenaiRes, {
                "Phase 9: OpenAI system still accessible": (r) => r && r.status === grpc.StatusOK,
                "Phase 9: OpenAI system unchanged": (r) => r.message.system.id === "openai",
            });

            console.log("Phase 9: Protected system verification complete");
        });

        console.log("\n=== All System Admin API tests completed successfully ===");
    });
}
