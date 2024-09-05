/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.s3;

public final class S3FileSystemConstants
{
    public static final String EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY = "internal$s3_aws_access_key";
    public static final String EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY = "internal$s3_aws_secret_key";
    public static final String EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY = "internal$s3_aws_session_token";
    public static final String EXTRA_CREDENTIALS_SSEC_KEY = "internal$s3_sse_c_key";

    private S3FileSystemConstants() {}
}
