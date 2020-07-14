CREATE OR REPLACE PROCEDURE proc_insert_chao_mung_thanh_cong()
AS $$
DECLARE

BEGIN
	-- Chao mung thanh cong
	INSERT INTO mapping_changed_status_student ( student_id, change_status_date_id, to_status_id, timestamp1 ) SELECT DISTINCT
	student_id :: BIGINT,
	to_char( ngay_a1, 'yyyymmdd' ) :: BIGINT AS change_status_date_id,
	2 AS to_status_id,
	dateadd ( M,- 60, ngay_a1 ) 
	FROM
		temp_ls_trang_thai_a1 
	WHERE
		student_id ~ '^[0-9]+$';
	-- Chao mung thanh cong trong 24h tu luc dong tien
	INSERT INTO mapping_changed_status_student ( student_id, change_status_date_id, to_status_id, measure2 ) SELECT DISTINCT
	ta1.student_id :: BIGINT,
	to_char( ngay_a1, 'yyyymmdd' ) :: BIGINT AS change_status_date_id,
	3 AS to_status_id,
	DATEDIFF ( mins, mcss.timestamp1, ta1.ngay_a1 ) AS measure2 
	FROM
		temp_ls_trang_thai_a1 ta1
		JOIN mapping_changed_status_student mcss ON mcss.student_id = ta1.student_id 
		AND mcss.to_status_id = 2 
		AND ta1.ngay_a1 <= mcss.timestamp1 + 1 
		AND ta1.ngay_a1 >= mcss.timestamp1;

	UPDATE mapping_changed_status_student
	SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 2 AND source_id = student_id )
	WHERE
		user_id IS NULL;
END;
$$ LANGUAGE plpgsql;