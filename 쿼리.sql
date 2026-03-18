# 각 년도별 승하차 합계 view
--- 우린 테이블 다 나눠놔서 년도별로 from 바꿔서 다 넣어야함 
CREATE OR REPLACE VIEW vw_승차 AS
SELECT 날짜, 역번호, 역명, 구분,(
`05~06` +`06~07` +`07~08` +`08~09` +`09~10` +`10~11` +`11~12` +`12~13` +`13~14` +`14~15` +`15~16` +`16~17` +`17~18` +`18~19` +`19~20` +`20~21` +`21~22` +`22~23` +`23~24` +`24~` ) AS 인원
FROM `2008`
WHERE 구분 = '승차';

CREATE OR REPLACE VIEW vw_하차 AS
SELECT 날짜, 역번호, 역명, 구분,(
`05~06` +`06~07` +`07~08` +`08~09` +`09~10` +`10~11` +`11~12` +`12~13` +`13~14` +`14~15` +`15~16` +`16~17` +`17~18` +`18~19` +`19~20` +`20~21` +`21~22` +`22~23` +`23~24` +`24~` ) AS 인원
FROM `2008`
WHERE 구분 = '하차';

# 해당 날짜마다 / 역마다의 승하차 인원수 합계 보기
--- 위 아래 둘중 아무거나 가능
SELECT 날짜, 역번호, 역명, SUM(인원) AS 승차, SUM(인원) AS 하차
FROM (
SELECT * FROM vw_승차
UNION
SELECT * FROM vw_하차
) AS t
WHERE 날짜 = '2008-01-01'
GROUP BY 날짜, 역번호, 역명
;

SELECT 
    날짜, 역번호, 역명, 
    SUM(CASE WHEN 구분 = '승차' THEN 인원 ELSE 0 END) AS 승차,
    SUM(CASE WHEN 구분 = '하차' THEN 인원 ELSE 0 END) AS 하차
FROM (
    SELECT * FROM vw_승차
    UNION ALL
    SELECT * FROM vw_하차
) AS t
WHERE 날짜 = '2008-01-01'
GROUP BY 날짜, 역번호, 역명;
