<?php
/**
 * Plugin Name: GroMore Warren Publisher Endpoint
 * Description: Adds a small authenticated REST endpoint for GroMore/Warren publishing on hosts that block default WordPress REST post routes.
 * Version: 1.0.0
 */

if (!defined('ABSPATH')) {
    exit;
}

function gromore_warren_basic_auth_header($request) {
    $headers = array(
        $request->get_header('authorization'),
        $request->get_header('x_gm_auth'),
        isset($_SERVER['HTTP_AUTHORIZATION']) ? $_SERVER['HTTP_AUTHORIZATION'] : '',
        isset($_SERVER['REDIRECT_HTTP_AUTHORIZATION']) ? $_SERVER['REDIRECT_HTTP_AUTHORIZATION'] : '',
        isset($_SERVER['HTTP_X_GM_AUTH']) ? $_SERVER['HTTP_X_GM_AUTH'] : '',
    );

    foreach ($headers as $header) {
        $header = trim((string) $header);
        if (stripos($header, 'basic ') === 0) {
            return substr($header, 6);
        }
    }

    return '';
}

function gromore_warren_authenticate_request($request) {
    if (is_user_logged_in() && current_user_can('edit_posts')) {
        return true;
    }

    if (!function_exists('wp_authenticate_application_password')) {
        return new WP_Error(
            'gromore_app_passwords_unavailable',
            'WordPress Application Passwords are not available on this site.',
            array('status' => 401)
        );
    }

    $encoded = gromore_warren_basic_auth_header($request);
    if (!$encoded) {
        return new WP_Error(
            'gromore_missing_auth',
            'Missing WordPress Application Password authentication.',
            array('status' => 401)
        );
    }

    $decoded = base64_decode($encoded);
    if (!$decoded || strpos($decoded, ':') === false) {
        return new WP_Error(
            'gromore_bad_auth',
            'Invalid authentication header.',
            array('status' => 401)
        );
    }

    list($username, $password) = explode(':', $decoded, 2);
    $user = wp_authenticate_application_password(null, $username, $password);
    if (is_wp_error($user)) {
        return new WP_Error(
            'gromore_auth_failed',
            $user->get_error_message(),
            array('status' => 401)
        );
    }

    wp_set_current_user($user->ID);
    if (!current_user_can('edit_posts')) {
        return new WP_Error(
            'gromore_forbidden',
            'The authenticated user cannot create or edit posts.',
            array('status' => 403)
        );
    }

    return true;
}

function gromore_warren_publish($request) {
    $type = sanitize_key($request->get_param('type') ?: 'post');
    if (!in_array($type, array('post', 'page'), true)) {
        return new WP_Error('gromore_bad_type', 'type must be post or page.', array('status' => 400));
    }

    $allowed_statuses = array('draft', 'publish', 'pending', 'private');
    $status = sanitize_key($request->get_param('status') ?: 'draft');
    if (!in_array($status, $allowed_statuses, true)) {
        $status = 'draft';
    }

    $post_id = absint($request->get_param('id'));
    $postarr = array(
        'post_type' => $type,
        'post_title' => wp_strip_all_tags((string) $request->get_param('title')),
        'post_content' => wp_kses_post((string) $request->get_param('content')),
        'post_excerpt' => sanitize_textarea_field((string) $request->get_param('excerpt')),
        'post_status' => $status,
    );

    $slug = sanitize_title((string) $request->get_param('slug'));
    if ($slug) {
        $postarr['post_name'] = $slug;
    }

    if ($type === 'page') {
        $postarr['post_parent'] = absint($request->get_param('parent'));
    }

    if ($post_id) {
        $existing = get_post($post_id);
        if (!$existing || $existing->post_type !== $type) {
            return new WP_Error('gromore_missing_post', 'Post/page id was not found.', array('status' => 404));
        }
        if (!current_user_can('edit_post', $post_id)) {
            return new WP_Error('gromore_forbidden_edit', 'The authenticated user cannot edit this item.', array('status' => 403));
        }
        $postarr['ID'] = $post_id;
        $saved_id = wp_update_post($postarr, true);
    } else {
        if (!current_user_can($type === 'page' ? 'publish_pages' : 'publish_posts') && $status === 'publish') {
            return new WP_Error('gromore_forbidden_publish', 'The authenticated user cannot publish this item.', array('status' => 403));
        }
        $saved_id = wp_insert_post($postarr, true);
    }

    if (is_wp_error($saved_id)) {
        return $saved_id;
    }

    $featured_media = absint($request->get_param('featured_media'));
    if ($featured_media) {
        set_post_thumbnail($saved_id, $featured_media);
    }

    $meta = $request->get_param('meta');
    if (is_array($meta) && current_user_can('edit_post', $saved_id)) {
        foreach ($meta as $key => $value) {
            $key = sanitize_key($key);
            if ($key) {
                update_post_meta($saved_id, $key, sanitize_text_field((string) $value));
            }
        }
    }

    return rest_ensure_response(array(
        'ok' => true,
        'id' => (int) $saved_id,
        'type' => $type,
        'status' => get_post_status($saved_id),
        'link' => get_permalink($saved_id),
    ));
}

add_action('rest_api_init', function () {
    register_rest_route('warren/v1', '/publish', array(
        'methods' => 'POST',
        'callback' => 'gromore_warren_publish',
        'permission_callback' => 'gromore_warren_authenticate_request',
    ));
});
