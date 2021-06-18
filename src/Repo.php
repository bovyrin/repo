<?php

declare(strict_types=1);

namespace sbovyrin;

use Yii;
use yii\db\Query;
use yii\db\Command;
use yii\db\Migration;

class Repo
{
    static function vals(Query $q): array
    {
        $q->params = [];
        return $q->all();
    }

    static function get(string $name): Query
    {
        return (new Query)
            ->from("{{%{$name}}} {$name}")
            ->addParams(['name' => $name]);
    }

    static function filter(callable $p): callable
    {
        return static function (Query $q) use ($p): Query {
            $invoke = static function (callable $_p) use ($q, &$invoke) {
                return array_map(
                    static fn ($x) => is_callable($x) ? $invoke($x) : $x,
                    $_p($q->params['name'])
                );
            };

            return $q->andWhere($invoke($p));
        };
    }

    static function select(array $cols): callable
    {
        return static fn (Query $q): Query => $q->addSelect(
            array_merge(
                ...array_map(
                    static fn ($v, $k) => [$k => "{$q->params['name']}.{$v}"],
                    $cols,
                    array_keys($cols)
                )
            )
        );
    }

    static function unionBy(array $glue, Query $q1): callable
    {
        if (count($glue) < 2)
            throw new \TypeError('Arg1 expected [q1_key, q2_key]');

        return static function (Query $q2) use ($glue, $q1): Query {
            $q1->innerJoin(
                $q2->from[0],
                "{$q2->params['name']}.{$glue[1]}={$q1->params['name']}.{$glue[0]}"
            );

            if (!empty($q2->select)) $q1->addSelect($q2->select);

            if (!empty($q2->where)) $q1->andWhere($q2->where);

            if (!empty($q2->join)) $q1->join(...$q2->join[0]);

            return $q1;
        };
    }

    static function and($x, ...$xn)
    {
        return static fn () => ['and', $x, ...$xn];
    }

    static function or($x, ...$xn)
    {
        return static fn () => ['or', $x, ...$xn];
    }

    static function has(string $k, $v): callable
    {
        return static fn (string $a): array => [
            'in',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function eq(string $k, $v): callable
    {
        return static fn (string $a): array => [
            '=',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function gt(string $k, $v): callable
    {
        return static fn (string $a): array => [
            '>',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function gte(string $k, $v): callable
    {
        return static fn (string $a): array => [
            '>=',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function lt(string $k, $v): callable
    {
        return static fn (string $a): array => [
            '<',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function lte(string $k, $v): callable
    {
        return static fn (string $a): array => [
            '<=',
            join('.', [$a, $k]),
            $v
        ];
    }

    static function pattern(string $k, string $v): callable
    {
        $v = str_replace('*', '%', $v);

        return static fn (string $a): array => [
            'like',
            join('.', [$a, $k]),
            $v,
            false
        ];
    }

    static function sortBy(callable $f): callable
    {
        return static fn (Query $q): Query => $q->orderBy($f());
    }

    static function slice(int $limit, int $offset = null): callable
    {
        return static fn (Query $q): Query =>
            $q->limit($limit)->offset($offset);
    }

    static function len(Query $q): int
    {
        $q->params = [];
        return (int) $q->count(1);
    }

    static function insert(Query $q): callable
    {
        return static function (array $xs) use ($q): array {
            Yii::$app->db->createCommand()
                ->insert("{{%{$q->params['name']}}}", $xs)
                ->execute();

            return array_merge(
                ['id' => Yii::$app->db->getLastInsertID()],
                $xs
            );
        };
    }

    static function batchInsert(Query $q): callable
    {
        return static function (array $xs) use ($q): int {
            if (empty($xs)) return 0;

            Yii::$app->db->createCommand()
                ->batchInsert(
                    "{{%{$q->params['name']}}}",
                    array_keys($xs[0]),
                    $xs
                )
                ->execute();

            return count($xs);
        };
    }

    static function update(Query $q): callable
    {
        $toSql = static fn ($attrs) => Yii::$app->db->createCommand()
            ->update(
                "{{%{$q->params['name']}}}",
                $attrs,
                $q->where
            );

        $exec = static fn (Command $sql): int =>
            $sql->setRawSql(
                str_replace("`{$q->params['name']}`.", '', $sql->getRawSql())
            )->execute();

        return static fn ($xs) => $exec($toSql($xs));
    }

    static function isQuery($q): bool
    {
        return $q instanceof Query;
    }

    static function delete(Query $q): int
    {
        $toSql = static fn () => Yii::$app->db->createCommand()
            ->delete("{{%{$q->params['name']}}}", $q->where);

        $exec = static fn (Command $sql): int =>
            $sql->setRawSql(
                str_replace("`{$q->params['name']}`.", '', $sql->getRawSql())
            )->execute();

        return $exec($toSql());
    }

    static function atomic(callable $f): callable
    {
        return static fn (...$xs) =>
            Yii::$app->db->transaction(
                static fn () => $f(...$xs)
            );
    }

    static function toSql(Query $q): string
    {
        return $q->createCommand()->getRawSql();
    }

    static function init(string $name, array $schema): bool
    {
        $migration = new Migration();

        $fns = [
            'type' => [
                'text' => static fn ($col, ?int $max = 255) => $max > 255
                    ? $col->text($max)
                    : $col->string($max),
                'email' => static fn ($col, ?int $max = 255) => $col->string($max),
                'number' => static fn ($col, ?int $max = 0) => $col->integer(),
                'boolean' => static fn ($col, ?int $max = 0) => $col->boolean(),
            ],
            'required' => static fn ($col) => $col->notNull()
        ];

        $schemaToMigr = static function (array $fields, string $field) use (
            $schema,
            $fns,
            $migration
        ) {
            $fields[$field] = $fns['type'][$schema[$field]['type']](
                $migration,
                $schema[$field]['max'] ?? null
            );

            if ($schema[$field]['required']) {
                $fields[$field] = $fns['required'](
                    $fields[$field]
                );
            }

            return $fields;
        };

        try {
            self::len(self::get($name));
        } catch (\Throwable $e) {
            $migration->createTable(
                "{{%{$name}}}",
                array_reduce(
                    array_keys($schema),
                    $schemaToMigr,
                    ['id' => $migration->primaryKey()]
                )
            );
        }

        return true;
    }
}
