# Codex Project Rules (dockerstack-s3proxy)

These rules are mandatory for Codex in this project.

## 1) Always update `.opushforce.message`

- For every completed user task, Codex must update `.opushforce.message` before sending the final response.
- Do not skip this step, even for small edits.
- If no files were changed, still update `.opushforce.message` with a short note.

## 2) Message format

Use conversational summary content (not commit-style), aligned with what Codex says to the user.

Required structure in `.opushforce.message`:

1. Opening line confirming rule-following, for example:
   `feat:<tóm tắt nội dung>` and/or `fix:<tóm tắt nội dung>`
2. Section: `Input của user`
3. Section: `Nguyên nhân gốc`
4. Section: `Cách đã chỉnh để khắc phục`
5. Section: `File đã áp fix bug`
6. Section: `Trả lời câu hỏi "<câu hỏi chính của user>"`
7. If runtime action is needed, include exact command(s) at the end.

Formatting rules:

- Write in Vietnamese, concise and clear.
- Prefer short lines or flat bullets; no nested bullets.
- In `Input của user`, include the user's original request text; if too long, keep a concise excerpt that preserves key constraints.
- If user input contains secrets/tokens, mask sensitive values.
- File list can be plain filenames or paths.
- Do not use `<type>: <summary>` commit prefix.

## 3) Completion gate

Codex should treat task as incomplete until `.opushforce.message` is updated to reflect the latest work.
