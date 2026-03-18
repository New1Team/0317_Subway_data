# 매 년도별 테이블 생성
--- 합계 컬럼은 2017 이후로 없음
--- 합계 넣을거면 새로 추가해야함
USE seoul_metro;
seoul_metro
DROP TABLE `2015`;
TRUNCATE TABLE `2015`;

CREATE TABLE `2015`(
`날짜` VARCHAR(255),
`역번호` INT, 
`역명` VARCHAR(255), 
`구분` VARCHAR(255), 
`05~06` VARCHAR(255), 
`06~07` VARCHAR(255), 
`07~08` VARCHAR(255), 
`08~09` VARCHAR(255), 
`09~10` VARCHAR(255), 
`10~11` VARCHAR(255), 
`11~12` VARCHAR(255), 
`12~13` VARCHAR(255), 
`13~14` VARCHAR(255), 
`14~15` VARCHAR(255), 
`15~16` VARCHAR(255), 
`16~17` VARCHAR(255), 
`17~18` VARCHAR(255), 
`18~19` VARCHAR(255), 
`19~20` VARCHAR(255), 
`20~21` VARCHAR(255), 
`21~22` VARCHAR(255), 
`22~23` VARCHAR(255),
`23~24` VARCHAR(255), 
`24~` VARCHAR(255)
);

LOAD DATA LOCAL INFILE 'C:\\Users\\hi\\Downloads\\2015.csv'
IGNORE INTO TABLE `seoul_metro`.`2015`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
 @v날짜, @v역번호, @v역명, @v구분, 
 @v05, @v06, @v07, @v08, @v09, @v10, @v11, @v12, 
 @v13, @v14, @v15, @v16, @v17, @v18, @v19, @v20, 
 @v21, @v22, @v23, @v24
)
SET 
 `날짜`   = TRIM(REPLACE(@v날짜, '"', '')),
 `역번호` = TRIM(REPLACE(@v역번호, '"', '')),
 `역명`   = TRIM(REPLACE(@v역명, '"', '')),
 `구분`   = TRIM(REPLACE(@v구분, '"', '')),
 `05~06` = REPLACE(@v05, ',', ''), -- 숫자 안의 쉼표 제거
 `06~07` = REPLACE(@v06, ',', ''),
 `07~08` = REPLACE(@v07, ',', ''),
 `08~09` = REPLACE(@v08, ',', ''),
 `09~10` = REPLACE(@v09, ',', ''),
 `10~11` = REPLACE(@v10, ',', ''),
 `11~12` = REPLACE(@v11, ',', ''),
 `12~13` = REPLACE(@v12, ',', ''),
 `13~14` = REPLACE(@v13, ',', ''),
 `14~15` = REPLACE(@v14, ',', ''),
 `15~16` = REPLACE(@v15, ',', ''),
 `16~17` = REPLACE(@v16, ',', ''),
 `17~18` = REPLACE(@v17, ',', ''),
 `18~19` = REPLACE(@v18, ',', ''),
 `19~20` = REPLACE(@v19, ',', ''),
 `20~21` = REPLACE(@v20, ',', ''),
 `21~22` = REPLACE(@v21, ',', ''),
 `22~23` = REPLACE(@v22, ',', ''),
 `23~24` = REPLACE(@v23, ',', ''),
 `24~`   = REPLACE(@v24, ',', '');
 
 UPDATE `2015` SET 
`05~06` = REPLACE(`05~06`, '"', ''), 
`06~07` = REPLACE(`06~07`, '"', ''), 
`07~08` = REPLACE(`07~08`, '"', ''), 
`08~09` = REPLACE(`08~09`, '"', ''), 
`09~10` = REPLACE(`09~10`, '"', ''), 
`10~11` = REPLACE(`10~11`, '"', ''), 
`11~12` = REPLACE(`11~12`, '"', ''), 
`12~13` = REPLACE(`12~13`, '"', ''), 
`13~14` = REPLACE(`13~14`, '"', ''), 
`14~15` = REPLACE(`14~15`, '"', ''), 
`15~16` = REPLACE(`15~16`, '"', ''), 
`16~17` = REPLACE(`16~17`, '"', ''), 
`17~18` = REPLACE(`17~18`, '"', ''), 
`18~19` = REPLACE(`18~19`, '"', ''), 
`19~20` = REPLACE(`19~20`, '"', ''), 
`20~21` = REPLACE(`20~21`, '"', ''), 
`21~22` = REPLACE(`21~22`, '"', ''), 
`22~23` = REPLACE(`22~23`, '"', ''),
`23~24` = REPLACE(`23~24`, '"', ''),
`24~`= REPLACE(`24~`, '"', '');

SELECT s.`날짜` FROM edu.seoul_metro2 AS s
WHERE s.날짜;

