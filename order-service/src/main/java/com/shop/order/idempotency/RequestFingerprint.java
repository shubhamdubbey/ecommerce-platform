package com.shop.order.idempotency;

import com.shop.order.api.CreateOrderRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Builds a stable fingerprint of what an order request MEANS, ignoring JSON formatting.
 *
 * Explanation: we hash a canonical string built from the fields that matter (customer, currency, the ordered list of sku and quantity), not the raw JSON.
 * So {"qty":1,"sku":"A"} and { "sku": "A", "qty": 1 } produce the same fingerprint, while changing a quantity changes it. SHA-256 gives a fixed-size,
 * collision-resistant value we can compare cheaply. Item order counts, which is right: a retry sends the same list.
 * */
public final class RequestFingerprint {

    private RequestFingerprint() {
    }

    public static String of(CreateOrderRequest request) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(request.customerId())
                .append('|').append(request.currency().toUpperCase());
        for (CreateOrderRequest.Item item : request.items()) {
            canonical.append('|').append(item.sku()).append(':').append(item.qty());
        }
        return sha256Hex(canonical.toString());
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}