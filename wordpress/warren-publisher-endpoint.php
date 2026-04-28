<?php
/**
 * Plugin Name: GroMore Warren Publisher Endpoint
 * Description: Adds authenticated GroMore/Warren publishing endpoints and pull publishing for hosts that block inbound publish requests.
 * Version: 1.3.1
 */

if (!defined('ABSPATH')) {
    exit;
}

function gromore_warren_basic_auth_header($request = null) {
    $headers = array(
        $request instanceof WP_REST_Request ? $request->get_header('authorization') : '',
        $request instanceof WP_REST_Request ? $request->get_header('x_gm_auth') : '',
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

function gromore_warren_authenticate_request($request = null) {
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

    $username = '';
    $password = '';
    $encoded = gromore_warren_basic_auth_header($request);
    if ($encoded) {
        $decoded = base64_decode($encoded);
        if (!$decoded || strpos($decoded, ':') === false) {
            return new WP_Error(
                'gromore_bad_auth',
                'Invalid authentication header.',
                array('status' => 401)
            );
        }
        list($username, $password) = explode(':', $decoded, 2);
    } else {
        $username = isset($_POST['gm_user']) ? sanitize_text_field(wp_unslash($_POST['gm_user'])) : '';
        $password = isset($_POST['gm_app_password']) ? sanitize_text_field(wp_unslash($_POST['gm_app_password'])) : '';
        if (!$username || !$password) {
            return new WP_Error(
                'gromore_missing_auth',
                'Missing WordPress Application Password authentication.',
                array('status' => 401)
            );
        }
    }

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

function gromore_warren_error_payload($error) {
    $data = $error->get_error_data();
    $status = is_array($data) && isset($data['status']) ? absint($data['status']) : 400;
    return array(
        'status' => $status ?: 400,
        'body' => array(
            'ok' => false,
            'code' => $error->get_error_code(),
            'message' => $error->get_error_message(),
        ),
    );
}

function gromore_warren_publish_payload_array($payload) {
    $request = new WP_REST_Request('POST', '/warren/v1/publish');
    foreach ($payload as $key => $value) {
        $request->set_param($key, $value);
    }
    return gromore_warren_publish($request);
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
        'post_status' => $status,
    );
    if ($request->get_param('title') !== null) {
        $postarr['post_title'] = wp_strip_all_tags((string) $request->get_param('title'));
    }
    if ($request->get_param('content') !== null) {
        $postarr['post_content'] = wp_kses_post((string) $request->get_param('content'));
    }
    if ($request->get_param('excerpt') !== null) {
        $postarr['post_excerpt'] = sanitize_textarea_field((string) $request->get_param('excerpt'));
    }

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

function gromore_warren_publish_from_post_payload() {
    $auth = gromore_warren_authenticate_request(null);
    if (is_wp_error($auth)) {
        $payload = gromore_warren_error_payload($auth);
        wp_send_json_error($payload['body'], $payload['status']);
    }

    $raw_payload = isset($_POST['payload']) ? wp_unslash($_POST['payload']) : '';
    $payload = json_decode((string) $raw_payload, true);
    if (!is_array($payload)) {
        wp_send_json_error(array(
            'ok' => false,
            'code' => 'gromore_bad_payload',
            'message' => 'Invalid publish payload.',
        ), 400);
    }

    $result = gromore_warren_publish_payload_array($payload);
    if (is_wp_error($result)) {
        $payload = gromore_warren_error_payload($result);
        wp_send_json_error($payload['body'], $payload['status']);
    }

    $response = rest_ensure_response($result);
    wp_send_json_success($response->get_data());
}

function gromore_warren_ajax_publish() {
    gromore_warren_publish_from_post_payload();
}

add_action('wp_ajax_gromore_warren_publish', 'gromore_warren_ajax_publish');
add_action('wp_ajax_nopriv_gromore_warren_publish', 'gromore_warren_ajax_publish');

function gromore_warren_front_publish() {
    if (!isset($_GET['gromore_warren_publish'])) {
        return;
    }
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        wp_send_json_error(array(
            'ok' => false,
            'code' => 'gromore_method_not_allowed',
            'message' => 'Use POST for GroMore publishing.',
        ), 405);
    }
    gromore_warren_publish_from_post_payload();
}

add_action('init', 'gromore_warren_front_publish');

function gromore_warren_pull_options() {
    $default_app_url = defined('GROMORE_WARREN_DEFAULT_APP_URL') ? GROMORE_WARREN_DEFAULT_APP_URL : '';
    $default_brand_id = defined('GROMORE_WARREN_DEFAULT_BRAND_ID') ? GROMORE_WARREN_DEFAULT_BRAND_ID : 0;

    return array(
        'app_url' => trim((string) get_option('gromore_warren_app_url', $default_app_url)),
        'brand_id' => absint(get_option('gromore_warren_brand_id', $default_brand_id)),
        'wp_user' => trim((string) get_option('gromore_warren_wp_user', '')),
        'wp_app_password' => trim((string) get_option('gromore_warren_wp_app_password', '')),
    );
}

function gromore_warren_pull_endpoint($path) {
    $opts = gromore_warren_pull_options();
    if (!$opts['app_url']) {
        return '';
    }
    return rtrim($opts['app_url'], '/') . $path;
}

function gromore_warren_pull_publish_once() {
    $opts = gromore_warren_pull_options();
    if (!$opts['app_url'] || !$opts['brand_id'] || !$opts['wp_user'] || !$opts['wp_app_password']) {
        return array('ok' => false, 'message' => 'GroMore pull settings are incomplete.');
    }

    $_POST['gm_user'] = $opts['wp_user'];
    $_POST['gm_app_password'] = $opts['wp_app_password'];
    $auth = gromore_warren_authenticate_request(null);
    if (is_wp_error($auth)) {
        return array('ok' => false, 'message' => $auth->get_error_message());
    }

    $body = array(
        'brand_id' => $opts['brand_id'],
        'wp_user' => $opts['wp_user'],
        'wp_app_password' => $opts['wp_app_password'],
    );

    $next = wp_remote_post(gromore_warren_pull_endpoint('/api/wordpress/pull/next'), array(
        'timeout' => 30,
        'body' => $body,
    ));
    if (is_wp_error($next)) {
        return array('ok' => false, 'message' => $next->get_error_message());
    }

    $next_data = json_decode(wp_remote_retrieve_body($next), true);
    if (!is_array($next_data) || empty($next_data['ok'])) {
        return array('ok' => false, 'message' => isset($next_data['error']) ? $next_data['error'] : 'GroMore pull returned an invalid response.');
    }
    if (empty($next_data['post']) || !is_array($next_data['post'])) {
        return array('ok' => true, 'message' => 'No queued GroMore posts found.');
    }

    $queued_post = $next_data['post'];
    $local_payload = array(
        'type' => 'post',
        'title' => isset($queued_post['title']) ? $queued_post['title'] : 'Untitled Post',
        'content' => isset($queued_post['content']) ? $queued_post['content'] : '',
        'excerpt' => isset($queued_post['excerpt']) ? $queued_post['excerpt'] : '',
        'slug' => isset($queued_post['slug']) ? $queued_post['slug'] : '',
        'status' => 'publish',
        'meta' => isset($queued_post['meta']) && is_array($queued_post['meta']) ? $queued_post['meta'] : array(),
    );

    $result = gromore_warren_publish_payload_array($local_payload);
    if (is_wp_error($result)) {
        wp_remote_post(gromore_warren_pull_endpoint('/api/wordpress/pull/complete'), array(
            'timeout' => 15,
            'body' => array_merge($body, array(
                'post_id' => absint($queued_post['id']),
                'status' => 'failed',
                'error' => $result->get_error_message(),
            )),
        ));
        return array('ok' => false, 'message' => $result->get_error_message());
    }

    $response = rest_ensure_response($result);
    $data = $response->get_data();
    wp_remote_post(gromore_warren_pull_endpoint('/api/wordpress/pull/complete'), array(
        'timeout' => 15,
        'body' => array_merge($body, array(
            'post_id' => absint($queued_post['id']),
            'status' => 'published',
            'wp_post_id' => absint($data['id']),
            'wp_post_url' => isset($data['link']) ? $data['link'] : get_permalink(absint($data['id'])),
        )),
    ));

    return array('ok' => true, 'message' => 'Published queued GroMore post: ' . $local_payload['title']);
}

function gromore_warren_pull_cron() {
    gromore_warren_pull_publish_once();
}

add_action('gromore_warren_pull_cron', 'gromore_warren_pull_cron');

function gromore_warren_cron_schedules($schedules) {
    if (!isset($schedules['gromore_five_minutes'])) {
        $schedules['gromore_five_minutes'] = array(
            'interval' => 5 * MINUTE_IN_SECONDS,
            'display' => 'Every 5 minutes',
        );
    }
    return $schedules;
}

add_filter('cron_schedules', 'gromore_warren_cron_schedules');

add_action('admin_init', 'gromore_warren_activate');

function gromore_warren_activate() {
    if (!wp_next_scheduled('gromore_warren_pull_cron')) {
        wp_schedule_event(time() + 60, 'gromore_five_minutes', 'gromore_warren_pull_cron');
    }
}

function gromore_warren_deactivate() {
    wp_clear_scheduled_hook('gromore_warren_pull_cron');
}

register_activation_hook(__FILE__, 'gromore_warren_activate');
register_deactivation_hook(__FILE__, 'gromore_warren_deactivate');

function gromore_warren_admin_menu() {
    add_options_page(
        'GroMore Publisher',
        'GroMore Publisher',
        'manage_options',
        'gromore-warren-publisher',
        'gromore_warren_settings_page'
    );
}

add_action('admin_menu', 'gromore_warren_admin_menu');

function gromore_warren_settings_page() {
    if (!current_user_can('manage_options')) {
        return;
    }

    $message = '';
    if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['gromore_warren_settings_nonce']) && wp_verify_nonce(sanitize_text_field(wp_unslash($_POST['gromore_warren_settings_nonce'])), 'gromore_warren_settings')) {
        update_option('gromore_warren_app_url', esc_url_raw(wp_unslash($_POST['gromore_warren_app_url'] ?? '')));
        update_option('gromore_warren_brand_id', absint($_POST['gromore_warren_brand_id'] ?? 0));
        update_option('gromore_warren_wp_user', sanitize_text_field(wp_unslash($_POST['gromore_warren_wp_user'] ?? '')));
        update_option('gromore_warren_wp_app_password', sanitize_text_field(wp_unslash($_POST['gromore_warren_wp_app_password'] ?? '')));
        $message = 'Settings saved.';
        if (isset($_POST['gromore_warren_pull_now'])) {
            $result = gromore_warren_pull_publish_once();
            $message = $result['message'];
        }
    }

    $opts = gromore_warren_pull_options();
    ?>
    <div class="wrap">
        <h1>GroMore Publisher</h1>
        <?php if ($message) : ?>
            <div class="notice notice-info"><p><?php echo esc_html($message); ?></p></div>
        <?php endif; ?>
        <form method="post">
            <?php wp_nonce_field('gromore_warren_settings', 'gromore_warren_settings_nonce'); ?>
            <table class="form-table" role="presentation">
                <tr>
                    <th scope="row"><label for="gromore_warren_app_url">GroMore App URL</label></th>
                    <td><input name="gromore_warren_app_url" id="gromore_warren_app_url" type="url" class="regular-text" value="<?php echo esc_attr($opts['app_url']); ?>" placeholder="https://your-gromore-app.onrender.com"></td>
                </tr>
                <tr>
                    <th scope="row"><label for="gromore_warren_brand_id">GroMore Brand ID</label></th>
                    <td><input name="gromore_warren_brand_id" id="gromore_warren_brand_id" type="number" class="small-text" value="<?php echo esc_attr($opts['brand_id']); ?>"></td>
                </tr>
                <tr>
                    <th scope="row"><label for="gromore_warren_wp_user">WordPress Username</label></th>
                    <td><input name="gromore_warren_wp_user" id="gromore_warren_wp_user" type="text" class="regular-text" value="<?php echo esc_attr($opts['wp_user']); ?>"></td>
                </tr>
                <tr>
                    <th scope="row"><label for="gromore_warren_wp_app_password">Application Password</label></th>
                    <td><input name="gromore_warren_wp_app_password" id="gromore_warren_wp_app_password" type="password" class="regular-text" value="<?php echo esc_attr($opts['wp_app_password']); ?>"></td>
                </tr>
            </table>
            <?php submit_button('Save Settings'); ?>
            <?php submit_button('Pull Queued Post Now', 'secondary', 'gromore_warren_pull_now', false); ?>
        </form>
    </div>
    <?php
}
